import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret

import airflow_utils

""" Ingests Q-reviews Results and populates it into BigQuery

Schedule: Every day
Workflow:
  1) Call the qreviews API and save the json data into gcs
  2) Transfers the data from gcs to bq

Expectation: run every day and append to relevant tables on BQ w/ the data we receive.
"""

gc_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-qreviews",
    key="key.json",
)

dbt_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)

dag = DAG(
    dag_id="qreviews_to_cbh_v2",
    description="Q-reviews Results -> BigQuery",
    start_date=datetime.datetime(2021, 1, 19, 1 + 5),  # ingests at 1AM EST
    schedule_interval="0 6 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

api_account_sid = secret.Secret(
    deploy_type="env",
    deploy_target="API_ACCOUNT_SID",
    secret="qreviews-prod-secrets",
    key="api_account_sid",
)
api_key = secret.Secret(
    deploy_type="env",
    deploy_target="API_KEY",
    secret="qreviews-prod-secrets",
    key="api_key",
)

BQ_PROJECT = "cityblock-data"
BQ_DATASET = "qreviews"

qreviews_to_bq = KubernetesPodOperator(
    image="gcr.io/cityblock-data/qreviews:latest",
    namespace="default",
    name="qreviews-to-bq",
    task_id="qreviews_to_bq",
    secrets=[gc_secret, api_account_sid, api_key],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./qreviews_to_bq.py",
        f"--project={BQ_PROJECT}",
        f"--dataset={BQ_DATASET}",
    ],
    dag=dag,
)

dbt_run_cmd = """
    dbt deps &&
    dbt run --model 'abs_member_surveys' 'mrt_member_surveys'
"""

dbt_run = KubernetesPodOperator(
    dag=dag,
    get_logs=True,
    secrets=[dbt_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
        "DBT_PROFILES_DIR": "/profiles",
    },
    task_id="dbt_run",
    name="dbt-run",
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_run_cmd],
    namespace="default",
    image="gcr.io/cityblock-orchestration/dbt:latest",
    image_pull_policy="Always",
)

qreviews_to_bq >> dbt_run
