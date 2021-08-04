import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

import airflow_utils

""" Ingests Able Health results and populates it into BigQuery

Schedule: Every day
Workflow:
  1) Iterates through Able Health (AH) endpoints and downloads CSV per endpoint and transfers it to GCS
  2) Does simple transformation to AH data on GCS and populates it to BQ

Status: This is run everyday with the assumption that Able Health has run metrics on the data we delivered to them
    the day prior. A lot of the properties are hardcoded in the Python script (BQ project and dataset) so these things
    must be considered if we want to configure the place the data will ultimately live.

Expectation: run every day and append to relevant tables on BQ w/ the data we receive. This works in conjunction with
 the `cbh_to_able` DAG
This workflow will be daily on a temporary basis until we can figure out a better diff strategy on sending/receiving
 data from Able Health

"""

gc_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-able-health",
    key="key.json",
)

dag = DAG(
    dag_id="able_to_cbh_v1",
    description="Able Health results -> BigQuery",
    start_date=datetime.datetime(2021, 1, 18, 14 + 5),  # ingests at 2PM Eastern
    schedule_interval="0 19 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

temp_volume_config = {"emptyDir": {}}
cbh_temp_volume = Volume(name="cbh-temp", configs=temp_volume_config)
cbh_temp_volume_mount = VolumeMount(
    name="cbh-temp", mount_path="/cbh-temp", sub_path="", read_only=False
)
user_env = secret.Secret(
    deploy_type="env",
    deploy_target="USER",
    secret="ablehealth-prod-secrets",
    key="user",
)
pass_env = secret.Secret(
    deploy_type="env",
    deploy_target="PASS",
    secret="ablehealth-prod-secrets",
    key="password",
)

qm_svc_api_key_env = secret.Secret(
    deploy_type="env",
    deploy_target="API_KEY_QM",
    secret="ablehealth-prod-secrets",
    key="api_key.able",
)

ablehealth_image = "us.gcr.io/cityblock-data/ablehealth:latest"

ah_to_gcs = KubernetesPodOperator(
    image="gcr.io/cloud-builders/gcloud-slim@sha256:0f87d3558b0bc18cde080c8606d27e34619c1a16badcb279b145a0885da9ea93",
    namespace="default",
    name="ah-to-gcs",
    task_id="ah_to_gcs",
    secrets=[gc_secret, user_env, pass_env],
    volumes=[cbh_temp_volume],
    volume_mounts=[cbh_temp_volume_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=["/bin/bash", "-c"],
    arguments=[
        (
            "for endpoint in measure_results risk_suspects risk_scores;"  # all three endpoints
            " do curl -u $USER:$PASS https://cityblock.ablehealth.com/exports/patients/$endpoint -L -o /cbh-temp/$endpoint.csv;"  # curl for file and write to temp volume
            " done;"
            " echo Will copy files to date folder: {{{{ next_ds }}}}"
            " && gsutil -m rsync -r -d /cbh-temp/ gs://{bucket}/ablehealth_results/{{{{ next_ds }}}}".format(
                bucket=airflow_utils.GCS_BUCKET
            )  # sync all files to GCS for date run
        )
    ],
    dag=dag,
)

BQ_PROJECT = "cityblock-data"
BQ_DATASET = "ablehealth_results"

gcs_to_bq = KubernetesPodOperator(
    image=ablehealth_image,
    namespace="default",
    name="gcs-to-bq",
    task_id="gcs_to_bq",
    secrets=[gc_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./gcs_to_bq.py",
        f"--project={BQ_PROJECT}",
        f"--bucket={airflow_utils.GCS_BUCKET}",
        f"--dataset={BQ_DATASET}",
        "--date={{ next_ds }}",  # same date as the write location from prev task
    ],
    dag=dag,
)

BUCKET_PROJECT = "cityblock-orchestration"
gcs_to_qm_svc = KubernetesPodOperator(
    image=ablehealth_image,
    namespace="default",
    name="gcs-to-qm-svc",
    task_id="gcs_to_qm_svc",
    secrets=[gc_secret, qm_svc_api_key_env],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./gcs_to_qm_service.py",
        "--able-result-date={{ next_ds }}",
        "--qm-svc-env=prod",
        f"--project={BUCKET_PROJECT}",
        f"--bucket={airflow_utils.GCS_BUCKET}",
    ],
    dag=dag,
)

ah_to_gcs >> gcs_to_bq
ah_to_gcs >> gcs_to_qm_svc
