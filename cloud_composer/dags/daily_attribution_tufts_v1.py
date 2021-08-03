import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow.operators.sensors import ExternalTaskSensor

from airflow_utils import default_args

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-daily-tufts",
    key="key.json",
)

dag = DAG(
    dag_id="daily_attribution_tufts_v1",
    description="Runs Daily Tufts Member Attribution after Load To Silver",
    start_date=datetime.datetime(
        2021, 1, 19, 9 + 5
    ),  # Process files at 9am EST, after daily job & monthly
    schedule_interval="0 14 * * *",
    default_args=default_args,
    catchup=False,
)

wait_for_silver_view = ExternalTaskSensor(
    task_id="wait_for_silver_member_demographics_month",
    external_dag_id="load_daily_tufts_v2",
    external_task_id="update_tufts_daily_silver_view",
    timeout=60 * 60,  # one hours in seconds
    pool="sensors",
    dag=dag,
)

daily_tufts_member_attribution = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="daily-tufts-member-attribution",
    task_id="daily_tufts_member_attribution",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/AttributionRunner",
        "--project=cityblock-data",
        "--deployTo=tuftsproduction",
        "--bigQueryProject=tufts-data",
        "--bigQueryDataset=silver_claims",
        "--environment=prod",
        "--tempLocation=gs://internal-tmp-cbh/temp",
    ],
)

wait_for_silver_view >> daily_tufts_member_attribution
