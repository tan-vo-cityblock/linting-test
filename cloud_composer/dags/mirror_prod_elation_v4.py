import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

import airflow_utils


""" Mirrors our Elation Hosted MySQL DB in BigQuery daily

DAG: The task is carried out by a `KubernetesPodOperator` and uses the `gcloud` image to run a bash script.

Schedule: This DAG runs every day at 08:00AM UTC / 04:00AM EDT.
Elation updates their hosted DB every day at 04:00AM UTC / 12:00AM EDT and the update takes approx 30 - 40 minutes to complete.

v3 of this DAG ran at 05:00AM UTC / 01:00AM EDT, however we experienced Elation server connection issues at that time that
was not re-producible at other manually triggered run times so moving the DAG to a later start time.

Workflow:
  1) Export the tables as csv files from Elation to GCS and load from GCS to Bigquery [elation_db_to_gcs_and_bq].
  2) After load to BQ, we kick-off [push_elation_patient_encounters_job] via scio-jobs image.
  3) After 2, we kick-off [run_commons_cache_ccd_encounter] for Commons to ingest the output of the previous step.
  3) Parallel to 2, and after load to BQ (1), we run and test dbt models based on mirrored tables.
  3) Parallel to 2, and after load to BQ (1), we confirm if the firewall rules need to be updated for
  App Engine from Airflow for the QM production instance. [update_ae_firewall_if_need_be]
  4) Kick off [elation-to-qm-service task] kicks off to post Elation tracked quality measure data to QM Service.
For more details and debugging tips see "Elation Mirror DAG" section in playbooks/elation_mirror.md.
"""

GCP_SECRET_PATH = "/var/secrets/google"

elation_mirror_svc_acct = secret.Secret(
    deploy_type="volume",
    deploy_target=GCP_SECRET_PATH,
    secret="tf-svc-prod-elation-mirror",
    key="key.json",
)

elation_worker_svc_acct = secret.Secret(
    deploy_type="volume",
    deploy_target=GCP_SECRET_PATH,
    secret="tf-svc-elation-worker",
    key="key.json",
)

qm_svc_api_key_env = secret.Secret(
    deploy_type="env",
    deploy_target="API_KEY_QM",
    secret="elation-prod-secrets",
    key="api_key.elation",
)

# Using Volume with volume_config dict because K8s detects when an ssh key is being stored
# and throws an error when the directory/file perms are not read-only.
# By default, secret.Secret sets chmod perms to 0644 which is allows writes.
elation_secret_name = "cbh-db-mirror-elation-prod-secrets"
elation_secrets_volume_config = {
    "secret": {
        "secretName": elation_secret_name,
        # note this is decimal notation but represents chmod 400 octal notation (User Read only). JSON specs support decimal notation whereas YAML specs support octal notation.
        "defaultMode": 256,
    }
}
elation_secrets_volume = Volume(
    name=elation_secret_name, configs=elation_secrets_volume_config
)
elation_secrets_volume_mount = VolumeMount(
    name=elation_secret_name,
    mount_path=f"/{elation_secret_name}",
    sub_path="",
    read_only=True,
)

# TODO: Remove aptible secrets when PLAT-1714 is merged
aptible_user_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_USER",
    secret="aptible-prod-secrets",
    key="username",
)

aptible_pass_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_PASSWORD",
    secret="aptible-prod-secrets",
    key="password",
)

dag = DAG(
    dag_id="prod_elation_mirroring_v4",
    description="Mirror elation hosted MySQL DB into BQ",
    # Should run every morning at 08:00AM UTC / 04:00AM EDT
    start_date=datetime.datetime(2021, 1, 19, 8),  # Ingest data at 7AM EST
    schedule_interval="0 8 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

MIRROR_PROJECT = "cbh-db-mirror-prod"
MIRROR_DATASET = "elation_mirror"
MIRROR_BUCKET = "cbh-export-elation-prod"
elation_db_to_gcs_and_bq = KubernetesPodOperator(
    name="elation-db-to-gcs-and-bq",
    task_id="elation_db_to_gcs_and_bq",
    namespace="default",
    # TODO: Rename image to be more generic i.e: sql-to-bq-mirroring
    image="gcr.io/cityblock-data/cloud-sql-to-bq-mirroring:latest",
    image_pull_policy="Always",
    secrets=[elation_mirror_svc_acct],
    volumes=[elation_secrets_volume],
    volume_mounts=[elation_secrets_volume_mount],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json",
        "ELATION_SECRET_PATH": f"/{elation_secret_name}/elation-mirror-secrets.json",
    },
    cmds=[
        "python",
        "elation_mysql_mirror.py",
        f"--project={MIRROR_PROJECT}",
        f"--dataset={MIRROR_DATASET}",
        f"--bucket={MIRROR_BUCKET}",
    ],
    startup_timeout_seconds=240,
    execution_timeout=datetime.timedelta(
        hours=1
    ),  # as of 9/15/2020, job normally takes 20-30 mins to run.
    dag=dag,
)


ENVIRONMENT = "prod"
DATAFLOW_PROJECT = "cityblock-data"
PATIENT_DATA_BUCKET = "gs://cityblock-production-patient-data"
MEDICAL_DATASET = "medical"
GCS_TEMP_LOCATION = "gs://cityblock-data-dataflow-temp/temp"
push_elation_patient_encounters_job = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="push-elation-patient-encounters",
    task_id="push_elation_patient_encounters",
    secrets=[elation_worker_svc_acct],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PushElationPatientEncounters",
        f"--environment={ENVIRONMENT}",
        f"--project={DATAFLOW_PROJECT}",
        "--runner=DataflowRunner",
        "--workerMachineType=n2-highcpu-8",
        "--jobName=push-elation-patient-encounters",
        f"--patientDataBucket={PATIENT_DATA_BUCKET}",
        f"--medicalDataset={MEDICAL_DATASET}",
        f"--tempLocation={GCS_TEMP_LOCATION}",
        "--experiments=upload_graph",
    ],
)

dbt_run_elation_cmd = """
    dbt deps &&
    dbt run -m abstractions.elation
"""
dbt_run_elation = airflow_utils.dbt_k8s_pod_op(
    dbt_cmd=dbt_run_elation_cmd, task_name="dbt_run_elation", dag=dag
)

dbt_test_elation_cmd = """
    dbt deps &&
    dbt test -m abstractions.elation
"""
dbt_test_elation = airflow_utils.dbt_k8s_pod_op(
    dbt_cmd=dbt_test_elation_cmd, task_name="dbt_test_elation", dag=dag
)

ABLE_BUCKET_PROJECT = "cityblock-orchestration"
elation_to_qm_svc = KubernetesPodOperator(
    image="gcr.io/cityblock-data/elation-utils:latest",
    namespace="default",
    name="elation-to-qm-svc",
    task_id="elation_to_qm_svc",
    secrets=[qm_svc_api_key_env, elation_worker_svc_acct],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./elation_to_qm_service.py",
        "--qm-svc-env=prod",
        f"--project={ABLE_BUCKET_PROJECT}",
    ],
    startup_timeout_seconds=240,
    dag=dag,
)

# TODO: Remove task when PLAT-1714 is merged
run_commons_cache_ccd_encounter = KubernetesPodOperator(
    dag=dag,
    name="run-commons-cache-ccd-encounter",
    task_id="run-commons-cache-ccd-encounter",
    image="gcr.io/cityblock-data/commons-aptible-ssh:latest",
    startup_timeout_seconds=240,
    image_pull_policy="Always",
    namespace="default",
    secrets=[aptible_user_env, aptible_pass_env],
    get_logs=False,
    arguments=["npm", "run", "jobs:cache-ccd-encounters:production"],
)

elation_db_to_gcs_and_bq >> push_elation_patient_encounters_job >> run_commons_cache_ccd_encounter
elation_db_to_gcs_and_bq >> dbt_run_elation >> dbt_test_elation
elation_db_to_gcs_and_bq >> elation_to_qm_svc
