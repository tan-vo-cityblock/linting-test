import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret

from airflow_utils import dbt_k8s_pod_op, default_args

""" 
Does a `dbt run` on an evening schedule
Workflow: Look at latest version of dbt project and run all models tagged as `evening`
"""

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)

dag = DAG(
    dag_id="dbt_evening_v1",
    description="dbt evening runs for relevant models that require daily updates (like tables)",
    start_date=datetime.datetime(2021, 5, 13, 0),  # 7PM EST (8PM EDT)
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
)

# profiles.yml is located at the root of the dbt project

dbt_run_evening_cmd = """
    dbt deps &&
    dbt run --model tag:evening+
"""
dbt_run_evening = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_evening_cmd, task_name="dbt_run_evening", dag=dag
)

dbt_test_evening_cmd = """
    dbt deps &&
    dbt test --model tag:evening+
"""
dbt_test_evening = dbt_k8s_pod_op(
    dbt_cmd=dbt_test_evening_cmd, task_name="dbt_test_evening", dag=dag
)

dbt_run_evening >> dbt_test_evening
