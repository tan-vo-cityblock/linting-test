from datetime import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import airflow_utils

""" Runs a ML algorithm to predict whether an sms message requires a response or not

Schedule: Every day
Workflow: 
  1) Runs a query on BQ of sms messages
  2) Uses a pre-trained ML model to predict whether each message requires a response or not
  2) Writes the results to a source table in GBQ

Status: this is expected to run after the nightly DBT refresh job.

Expectation: run every day and write output to a table in GBQ

"""

BQ_PROJECT = "cityblock-analytics"
DATASET_NAME = "src_communications"
TABLE_NAME = "sms_response_required_model_output"
GCP_ENV_VAR = {"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"}

gc_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-ml-labeling",
    key="key.json",
)

dag = DAG(
    dag_id="sms_response_v1",
    description="predicting whether response is required for sms messages",
    start_date=datetime(2021, 1, 19, 17),  # 12AM EST (1AM EDT)
    schedule_interval="0 5,17 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

dbt_refresh_completed = ExternalTaskSensor(
    task_id="wait_for_dbt_refresh",
    external_dag_id="dbt_nightly_v4",
    external_task_id="dbt_run_commons",
    timeout=60 * 60 * 5,  # 5 hours in seconds
    pool="sensors",
    dag=dag,
)

sms_response = KubernetesPodOperator(
    image="gcr.io/cityblock-data/sms-response-required:latest",
    namespace="default",
    name="sms-response",
    task_id="sms_response",
    secrets=[gc_secret],
    env_vars=GCP_ENV_VAR,
    image_pull_policy="Always",
    cmds=[
        "python3",
        "./prod_sms_response_required.py",
        "--min_prob=0.7",
        "--model=response_not_essential_2019-02-28_15-05-44.bin",
        f"--output_project_id={BQ_PROJECT}",
        f"--output_tbl_name={DATASET_NAME}.{TABLE_NAME}",
    ],
    dag=dag,
)

dbt_refresh_completed >> sms_response
