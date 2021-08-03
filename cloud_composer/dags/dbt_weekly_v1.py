import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret

from airflow_utils import dbt_k8s_pod_op, default_args

""" 
Does a `dbt run` on a weekly schedule

Workflow: Look at latest version of dbt project and run all models tagged as `weekly`

"""

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)

dag = DAG(
    dag_id="dbt_weekly_v1",
    description="dbt weekly runs for relevant models that require weekly updates (like tables)",
    start_date=datetime.datetime(2021, 1, 17, 0 + 5),  # 12AM EST (1AM EDT)
    schedule_interval="0 5 * * 5",
    default_args=default_args,
    catchup=False,
)

# profiles.yml is located at the root of the dbt project
dbt_run_weekly_sheets_cmd = """
    dbt deps &&
    dbt run --model tag:sheets,tag:weekly &&
    dbt test --model tag:sheets,tag:weekly
"""
dbt_run_weekly_sheets = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_weekly_sheets_cmd, task_name="dbt_run_weekly_sheets", dag=dag
)

dbt_seed_weekly_cmd = """
    dbt deps &&
    dbt seed --select tag:weekly
"""
dbt_seed_weekly = dbt_k8s_pod_op(
    dbt_cmd=dbt_seed_weekly_cmd,
    task_name="dbt_seed_weekly",
    dag=dag,
    trigger_rule="all_done",
)

dbt_run_weekly_cmd = """
    dbt deps &&
    dbt run --model tag:weekly+ --exclude tag:evening
"""
dbt_run_weekly = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_weekly_cmd, task_name="dbt_run_weekly", dag=dag
)

dbt_test_weekly_cmd = """
    dbt deps &&
    dbt test --model tag:weekly+ --exclude tag:evening
"""
dbt_test_weekly = dbt_k8s_pod_op(
    dbt_cmd=dbt_test_weekly_cmd, task_name="dbt_test_weekly", dag=dag
)

dbt_run_weekly_sheets >> dbt_seed_weekly
dbt_seed_weekly >> dbt_run_weekly
dbt_run_weekly >> dbt_test_weekly
