from datetime import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

""" Updates the Daily 

Schedule: Once a day
Workflow:
  1) Runs the daily Days Not At Home job and writes that table to BQ
  2) Runs the Days Not At Home combiner job and writes that table to BQ

Expectations:
  1) Updates tables for Days Not At Home metric

Changelog:
  v1) initial version only contains 2 tasks (daily and combiner)

"""
BQ_PROJECT = "cityblock-analytics"
DATASET_NAME = "medical_economics"
CONFIG_BUCKET = "cbh-analytics-configs"
GCP_ENV_VAR = {"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"}
gcp_creds = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dnah-job-runner",
    key="key.json",
)

dag = DAG(
    dag_id="dnah_daily_v1",
    description="",
    start_date=datetime(2021, 1, 19, 0 + 5),  # midnight EDT
    schedule_interval="0 5 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

dnah_daily = KubernetesPodOperator(
    dag=dag,
    get_logs=True,
    secrets=[gcp_creds],
    env_vars=GCP_ENV_VAR,
    task_id="dnah_daily",
    name="dnah-daily",
    cmds=[
        "python",
        "dah_daily.py",
        f"--project={BQ_PROJECT}",
        f"--dataset-id={DATASET_NAME}",
    ],
    namespace="default",
    image="us.gcr.io/cbh-git/days_at_facility:latest",
    image_pull_policy="Always",
)

dnah_combine = KubernetesPodOperator(
    dag=dag,
    get_logs=True,
    secrets=[gcp_creds],
    env_vars=GCP_ENV_VAR,
    task_id="dnah_combine",
    name="dnah-combine",
    cmds=[
        "python",
        "dah_combiner.py",
        f"--project={BQ_PROJECT}",
        f"--dataset-id={DATASET_NAME}",
        f"--bucket={CONFIG_BUCKET}",
    ],
    namespace="default",
    image="us.gcr.io/cbh-git/days_at_facility:latest",
    image_pull_policy="Always",
)

dnah_daily >> dnah_combine
