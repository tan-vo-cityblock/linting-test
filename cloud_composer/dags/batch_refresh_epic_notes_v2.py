import datetime

from airflow import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

"""Refreshes clinical data from Epic/Redox
TODO:
1. Create playbook
2. Update / remove external dependency doc string when PLAT-1714 is merged

Schedule: This DAG runs every day at 18:00 EDT/ 22:00 UTC and takes approx 2-3 hrs to run.

External Dependency: The DAG task `update_clinical_summaries` 
needs to run before DAG task prod_elation_mirroring_v4.run_commons_cache_ccd_encounter.

See playbooks/elation_mirror.md for more.
"""

GCP_SECRET_PATH = "/var/secrets/google"
redox_worker_svc_acct = secret.Secret(
    deploy_type="volume",
    deploy_target=GCP_SECRET_PATH,
    secret="tf-svc-redox-worker",
    key="key.json",
)

dag = DAG(
    dag_id="batch_refresh_clinical_summaries_v2",
    description="batch refreshes diagnoses lists, encounters lists, medications lists, and vital signs lists for "
    "members with appointments in past 2 weeks",
    start_date=datetime.datetime(
        2021, 1, 19, 23
    ),  # running at 6pm EST/ 23 UTC; evening b/c the batch job seems to complete more requests to Redox then vs later in the evening at 9pm.
    schedule_interval="0 23 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

ENVIRONMENT = "prod"
DATAFLOW_PROJECT = "cityblock-data"
PATIENT_DATA_BUCKET = "gs://cityblock-production-patient-data"
MEDICAL_DATASET = "medical"
CLINICAL_INFO_UPDATE_TOPIC = "clinicalInformationUpdateMessages"
REDOX_MESSAGES_TABLE = "redox_messages"
GCS_TEMP_LOCATION = "gs://cityblock-data-dataflow-temp/temp"
update_clinical_summaries = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="batch-update-redox-clinical-summaries",
    task_id="batch_update_redox_clinical_summaries",
    secrets=[redox_worker_svc_acct],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/ClinicalSummaryRefresher",
        f"--environment={ENVIRONMENT}",
        f"--project={DATAFLOW_PROJECT}",
        "--runner=DataflowRunner",
        "--workerMachineType=n2-highcpu-16",
        "--jobName=batch-update-redox-clinical-summaries",
        "--streaming=false",
        f"--patientDataBucket={PATIENT_DATA_BUCKET}",
        f"--clinicalInfoUpdateTopic={CLINICAL_INFO_UPDATE_TOPIC}",
        f"--redoxMessagesTable={REDOX_MESSAGES_TABLE}",
        f"--medicalDataset={MEDICAL_DATASET}",
        f"--tempLocation={GCS_TEMP_LOCATION}",
    ],
    dag=dag,
)
