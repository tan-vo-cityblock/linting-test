import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow.operators.sensors import ExternalTaskSensor

from airflow_utils import default_args

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-emblem",
    key="key.json",
)

dag = DAG(
    dag_id="monthly_attribution_emblem_digital_v1",
    description="BigQuery (Silver) -> Member Service -> Commons",
    start_date=datetime.datetime(2021, 1, 19, 7 + 5),
    schedule_interval="0 12 * * *",
    default_args=default_args,
    catchup=False,
)

wait_for_silver_view = ExternalTaskSensor(
    task_id="wait_for_silver_member_demographics_month",
    external_dag_id="load_monthly_virtual_data_emblem_v1",
    external_task_id="update_emblem_digital_monthly_silver_view",
    timeout=60 * 60,  # one hours in seconds
    pool="sensors",
    dag=dag,
)

monthly_emblem_digital_member_attribution = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="daily-emblem-digital-member-attribution",
    task_id="daily_emblem_digital_member_attribution",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/AttributionRunner",
        "--project=cityblock-data",
        "--deployTo=emblemvirtualproduction",
        "--bigQueryProject=emblem-data",
        "--bigQueryDataset=silver_claims_virtual",
        "--environment=prod",
        "--tempLocation=gs://internal-tmp-cbh/temp",
    ],
)

wait_for_silver_view >> monthly_emblem_digital_member_attribution
