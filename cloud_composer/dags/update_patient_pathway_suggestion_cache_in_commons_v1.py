import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import airflow_utils

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
    dag_id="update_patient_pathway_suggestion_cache_in_commons_v1",
    description="weekly run to update the patient_pathway_suggestion cache table in Commons",
    start_date=datetime.datetime(2021, 1, 17, 0 + 5),  # 12AM EST (1AM EDT)
    schedule_interval="0 5 * * 5",
    default_args=airflow_utils.default_args,
    catchup=False,
)

dbt_databases_processed = ExternalTaskSensor(
    task_id="wait_for_dbt_model_updates",
    external_dag_id="dbt_weekly_v1",
    external_task_id="dbt_test_weekly",
    timeout=60 * 60 * 0.5,  # 30 min in seconds
    pool="sensors",
    dag=dag,
)

update_patient_pathway_suggestion_cache = KubernetesPodOperator(
    dag=dag,
    name="update-patient-pathway-suggestions",
    task_id="update-patient-pathway-suggestions",
    image="gcr.io/cityblock-data/commons-aptible-ssh:latest",
    startup_timeout_seconds=240,
    image_pull_policy="Always",
    namespace="default",
    secrets=[aptible_user_env, aptible_pass_env],
    arguments=["npm", "run", "update-pathway-suggestion-cache:production"],
)

dbt_databases_processed >> update_patient_pathway_suggestion_cache
