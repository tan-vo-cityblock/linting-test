import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

"""PROD: Bulk exports Zendesk models via Zendesk API bulk endpoints.

DAG: The task is carried out by a `KubernetesPodOperator`
and uses the `us.gcr.io/cityblock-data/zendesk:latest` image to run the python script.

Schedule: Every 6 hours starting at 18:00 EDT / 22:00 UTC.

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
    secret="tf-svc-prod-zendesk-worker",
    key="key.json",
)

api_creds_secret_name = "prod-zendesk-api-creds"
zendesk_api_creds = secret.Secret(
    deploy_type="volume",
    deploy_target=api_creds_secret_name,
    secret=api_creds_secret_name,
    key="latest",
)

dag = DAG(
    dag_id="prod_zendesk_bulk_export_v1",
    description="Bulk Export Zendesk models with bulk endpoints",
    # Should run every 6 hours starting at 18:00 EST / 23:00 UTC
    start_date=datetime.datetime(2021, 1, 20, 23),
    schedule_interval="0 */6 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

EXPORT_PROJECT = "cityblock-data"
EXPORT_DATASET = "zendesk"
EXPORT_BUCKET = "zendesk_export_prod"
bulk_export_to_gcs_bq = KubernetesPodOperator(
    name="prod-zendesk-bulk-export-default",
    task_id="prod_zendesk_bulk_export_default",
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
