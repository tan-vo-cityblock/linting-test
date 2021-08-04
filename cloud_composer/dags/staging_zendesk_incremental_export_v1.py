import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG

import airflow_utils

"""STAGING: Incrementally exports Zendesk models via Zendesk API incremental endpoints.

DAG: The task is carried out by a `KubernetesPodOperator`
and uses the `us.gcr.io/cityblock-data/zendesk:latest` image to run the python script.

Schedule: Every day starting at 11:00AM EDT / 15:00PM UTC.
Note that the prod DAG tasks will be updated to run more frequently for freshness.

Workflow:
  - Tasks 'increment_export_[model]' incrementally exports the Zendesk models
    in chunks to GCS as json files.
  - Once all model chunks are exported to GCS, the json files are composed together to one json file.
  - This composed json file is then loaded (append only) to standard BQ tables.
  - We append b/c we are incrementally exporting using cursors provided by Zendesk API.
  - Task 'publish_tickets_to_pubsub' uses the `zendesk.ticket_latest` view to
    pull down tickets, lightly transform them, and publish them to PubSub for Commons use.
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
    dag_id="staging_zendesk_incremental_export_v1",
    description="Incrementally export Zendesk models via incremental endpoints",
    # Should run every day at 12:00PM EST / 17:00PM UTC.
    start_date=datetime.datetime(2021, 1, 19, 17),
    schedule_interval="0 17 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

EXPORT_PROJECT = "staging-cityblock-data"
EXPORT_DATASET = "zendesk"
EXPORT_BUCKET = "zendesk_export_staging"


def _get_k8_operator_for_model(model_name: str):
    return KubernetesPodOperator(
        name=f"staging-zendesk-incremenal-export-{model_name}",
        task_id=f"staging_zendesk_incremental_export_{model_name}",
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
            "--incremental_export",
            "--incremental_models",
            f"{model_name}",
        ],
        startup_timeout_seconds=240,
        dag=dag,
    )


# TODO: Add export tasks for ticket_event and ticket_audit models when PLAT-1868 is merged
increment_export_user = _get_k8_operator_for_model("user")
increment_export_organization = _get_k8_operator_for_model("organization")
increment_export_nps_recipient = _get_k8_operator_for_model("nps_recipient")
increment_export_nps_response = _get_k8_operator_for_model("nps_response")
increment_export_ticket = _get_k8_operator_for_model("ticket")

PUBSUB_DATASET = "pubsub_messages"
PUBUSUB_TOPIC = "stagingZendeskTicket"
publish_tickets_to_pubsub = KubernetesPodOperator(
    name="staging-publish-tickets-to-pubsub",
    task_id="staging_publish_tickets_to_pubsub",
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
        "publish_tickets.py",
        f"--zendesk_project={EXPORT_PROJECT}",
        f"--zendesk_dataset={EXPORT_DATASET}",
        f"--pubsub_project={EXPORT_PROJECT}",
        f"--pubsub_dataset={PUBSUB_DATASET}",
        f"--pubsub_topic={PUBUSUB_TOPIC}",
    ],
    startup_timeout_seconds=240,
    dag=dag,
)

increment_export_ticket >> publish_tickets_to_pubsub
