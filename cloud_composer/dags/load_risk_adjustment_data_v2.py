import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import airflow_utils

""" Runs tasks to dump necessary BigQuery datasets into GCS flat files

Schedule: Every day

Workflow:
  1) Wait until dbt_evening_v1 dag finishes
  2) If 1 fails, fail the dag, if not, continue to 3, 4
  3) Loads cityblock-analytics.mrt_risk.mrt_risk_opportunities to
     gs://cityblock-production-patient-data/risk-adjustment/risk-adjustment-diagnoses.json
  5) Send Slack notification to stakeholders if job fails]

Expectations: 
  1) Any scheduling changes to dbt_evening_v1 will be reflected here, in order to preserve the sensor connection. 
  2) dbt_evening_v1 will complete in less than four hours, which is dependent on mirror_prod_commons_v1 completing.
  
"""

aptible_user_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_USER",
    secret="aptible-prod-secrets",
    key="username",
)

aptible_pass_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_PASSWORD",
    secret="aptible-prod-secrets",
    key="password",
)

dag = DAG(
    dag_id="load_risk_adjustment_data_v2",
    description="nightly runs to dump bq datasets to GCS buckets for consumption by Commons",
    start_date=datetime.datetime(2021, 7, 15, 0),  # 7PM EST (8PM EDT)
    schedule_interval="0 0 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

dbt_databases_processed = ExternalTaskSensor(
    task_id="wait_for_dbt_model_updates",
    external_dag_id="dbt_evening_v1",
    external_task_id="dbt_run_evening",
    timeout=60 * 240,  # four hours in seconds
    pool="sensors",
    dag=dag,
)

risk_adjustment = BigQueryToCloudStorageOperator(
    task_id="risk_adjustment_data_dump",
    source_project_dataset_table="cityblock-analytics:mrt_risk.mrt_risk_opportunities",
    destination_cloud_storage_uris=[
        "gs://cityblock-production-patient-data/risk-adjustment/"
        "risk-adjustment-diagnoses.json"
    ],
    export_format="NEWLINE_DELIMITED_JSON",
    bigquery_conn_id="biquery_to_gcs",
    dag=dag,
)

run_commons_ingestion = KubernetesPodOperator(
    dag=dag,
    name="run-commons-ingestion",
    task_id="run-commons-ingestion",
    image="gcr.io/cityblock-data/commons-aptible-ssh:latest",
    startup_timeout_seconds=240,
    image_pull_policy="Always",
    namespace="default",
    secrets=[aptible_user_env, aptible_pass_env],
    get_logs=False,
    arguments=["npm", "run", "jobs:update-diagnoses-opportunities:production"],
)

dbt_databases_processed >> risk_adjustment >> run_commons_ingestion
