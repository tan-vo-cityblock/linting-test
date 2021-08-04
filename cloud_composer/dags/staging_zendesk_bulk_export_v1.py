import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

"""STAGING: Bulk exports Zendesk models via Zendesk API bulk endpoints.

DAG: The task is carried out by a `KubernetesPodOperator`
and uses the `us.gcr.io/cityblock-data/zendesk:latest` image to run the python script.

Schedule: Every day starting at 12:00PM EDT / 16:00PM UTC. Note that the prod DAG runs
more frequently for freshness.

Workflow:
  - Task 'export_to_gcs_bq' bulk exports the Zendesk models in chunks to GCS as json files.
  - Once all model chunks are exported to GCS, the json files are composed together as one file.
  - This composed json file is then loaded to day-partitioned BQ tables.
  - Each interval the job runs will overwrite the table partition for the given day.
"""

GCP_SECRET_PATH = "/var/secrets/google"

zendesk_worker_svc_acct = secret.Secret(
    deploy_type="volume",
    deploy_target=GCP_SECRET_PATH,
    secret="tf-svc-staging-zendesk-worker",
    key="key.json",
)

api_creds_secret_name = "staging-zendesk-api-creds"
zendesk_api_creds = secret.Secret(
    deploy_type="volume",
    deploy_target=api_creds_secret_name,
    secret=api_creds_secret_name,
    key="latest",
)

dag = DAG(
    dag_id="staging_zendesk_bulk_export_v1",
    description="Bulk Export Zendesk models with bulk endpoints",
    # Should run every day at 12:00PM EST / 17:00PM UTC.
    # Note prod runs more frequently but this is staging so freshness not needed.
    start_date=datetime.datetime(2021, 1, 19, 17),
    schedule_interval="0 17 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

EXPORT_PROJECT = "staging-cityblock-data"
EXPORT_DATASET = "zendesk"
EXPORT_BUCKET = "zendesk_export_staging"
bulk_export_to_gcs_bq = KubernetesPodOperator(
    name="staging-zendesk-bulk-export-default",
    task_id="staging_zendesk_bulk_export_default",
    namespace="default",
    image="us.gcr.io/cityblock-data/zendesk:latest",
    image_pull_policy="Always",
    secrets=[zendesk_worker_svc_acct, zendesk_api_creds],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json",
        "ZENDESK_SECRET_PATH": f"/{api_creds_secret_name}/latest",
    },
    cmds=[
        "python",
        "run_export.py",
        f"--project={EXPORT_PROJECT}",
        f"--dataset={EXPORT_DATASET}",
        f"--bucket={EXPORT_BUCKET}",
        "--bulk_export",
    ],
    startup_timeout_seconds=240,
    dag=dag,
)
