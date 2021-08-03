from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from airflow_utils import (
    GCP_ENV_VAR,
    PROD_DBT_CREDS,
    cf_email,
    dbt_k8s_pod_op,
    default_args,
)

""" Does a `dbt run` at midnight and noon every day, after commons mirror completes

Schedule: Twice every day (following prod_commons_mirroring_v4)

Workflow:
  1) Wait until prod_commons_mirroring_v4 dag finishes
  2) If 1 fails, fail the dag, if not, continue to 3
  3) Look at latest version of dbt project and run all models tagged as `nightly`
  4) Run the computed fields (CF) Scala job
  5) Run query to see if CF data is refreshed and write date to XCom
  6) Notify DA if data is not refreshed (no new CF data)

Expectations:
  1) Any scheduling changes to prod_commons_mirroring_v4 will be reflected here, in order to preserve the sensor connection.
  2) prod_commons_mirroring_v4 will complete in less than 5 hours.

Changelog:
  v4.3) split up dbt run with dbt test for nightly
  v4.2) added SLI tracking
  v4.1) added CF alerting 
  v4) update start date
  v3.2) updated schedule to be once every 12 hours
  v3.1) added CF job
  v3) Added sensor to prod_commons_mirroring_v2, updated start time to match
  v2) Moved start time from 9 am to 6 am

"""

dataflow_creds = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-dataflow-runner-cityblock-data",
    key="key.json",
)

sli_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-sli-metrics",
    key="key.json",
)

dag = DAG(
    dag_id="dbt_nightly_v4",
    description="dbt nightly runs for relevant models that require daily updates (like tables)",
    start_date=datetime(2021, 1, 19, 17),  # 12AM EST (1AM EDT)
    schedule_interval="0 5,17 * * *",
    default_args=default_args,
    catchup=False,
)

prod_mirror_completed = ExternalTaskSensor(
    task_id="wait_for_prod_mirror",
    external_dag_id="prod_commons_mirroring_v4",
    external_task_id="update-bq-views",
    timeout=60 * 60 * 5,  # 5 hours in seconds
    pool="sensors",
    dag=dag,
)

# profiles.yml is located at the root of the dbt project
dbt_run_sheets_cmd = """
    dbt deps &&
    dbt run --model tag:sheets --exclude tag:weekly &&
    dbt test --model +tag:sheets --exclude tag:weekly
"""
dbt_run_sheets = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_sheets_cmd, task_name="dbt_run_sheets", dag=dag
)

dbt_seed_and_snapshot_cmd = """
    dbt deps &&
    dbt seed --exclude tag:weekly &&
    dbt snapshot
"""
dbt_seed_and_snapshot = dbt_k8s_pod_op(
    dbt_cmd=dbt_seed_and_snapshot_cmd,
    task_name="dbt_seed_and_snapshot",
    dag=dag,
    trigger_rule="all_done",
)

dbt_run_nightly_cmd = """
    dbt deps &&
    dbt run --model tag:nightly+ --exclude source:commons+ tag:weekly &&
    dbt test --model tag:nightly+ --exclude source:commons+ tag:weekly
"""
dbt_run_nightly = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_nightly_cmd, task_name="dbt_run_nightly", dag=dag
)

dbt_run_commons_cmd = """
    dbt deps &&
    dbt run --model source:commons+ --exclude tag:weekly tag:evening
"""
dbt_run_commons = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_commons_cmd, task_name="dbt_run_commons", dag=dag
)
dbt_test_commons_cmd = """
    dbt deps &&
    dbt test --model source:commons+ --exclude tag:weekly tag:evening
"""
dbt_test_commons = dbt_k8s_pod_op(
    dbt_cmd=dbt_test_commons_cmd, task_name="dbt_test_commons", dag=dag
)

SLI_PROJECT = "cbh-sre"
SLI_DATASET = "sli_metrics"
calculate_age_sli = KubernetesPodOperator(
    dag=dag,
    name="calculate-age-sli",
    task_id="calculate_age_sli",
    image="us.gcr.io/cityblock-data/slo-tools:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[sli_secret],
    env_vars=GCP_ENV_VAR,
    get_logs=True,
    cmds=[
        "python",
        "./db_mirror/age_metric.py",
        "--db=commons",
        f"--project={SLI_PROJECT}",
        f"--dataset-id={SLI_DATASET}",
    ],
)

dbt_run_other_computed_cmd = """
    dbt deps &&
    dbt run --model abstractions.computed.* --exclude source:commons+ tag:nightly+ &&
    dbt test --model abstractions.computed.* --exclude source:commons+ tag:nightly+
"""
dbt_run_other_computed = dbt_k8s_pod_op(
    dbt_cmd=dbt_run_other_computed_cmd, task_name="dbt_run_other_computed", dag=dag
)

computed_fields_run = KubernetesPodOperator(
    dag=dag,
    secrets=[dataflow_creds],
    env_vars=GCP_ENV_VAR,
    task_id="computed_fields_run",
    name="computed-fields-run",
    cmds=["bin/NewProductionResults"],
    arguments=["--runner=DataflowRunner", "--project=cityblock-data"],
    namespace="default",
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    image_pull_policy="Always",
)

query = "SELECT MAX(CAST(time as DATE)) AS latest_date FROM cityblock-data.computed_fields.results"
bq_project = "cityblock-analytics"
get_latest_cf_result = KubernetesPodOperator(
    # pinning image to release from May 18th 2020
    image="gcr.io/cloud-builders/gcloud-slim@sha256:19665afd1e3deb791abd39dba1737c46322b3e3ff28f591d831f377fb90b7af6",
    namespace="default",
    name="get-latest-cf-result",
    task_id="get_latest_cf_result",
    secrets=[PROD_DBT_CREDS],
    env_vars=GCP_ENV_VAR,
    image_pull_policy="Always",
    cmds=["/bin/bash", "-c"],
    arguments=[
        (
            "gcloud auth activate-service-account --key-file=/var/secrets/google/key.json"
            " && mkdir -p /airflow/xcom/"
            # need to do this hack to initialize the BQ CLI (start it). Alternative is to provide the BigQuery RC
            f" && bq --project_id={bq_project} head -n=1 'cityblock-data:computed_fields.results'"
            " && sleep 2"
            f" && bq --project_id={bq_project} --format=json query --use_legacy_sql=false '{query}' > /airflow/xcom/return.json"
        )
    ],
    do_xcom_push=True,
    dag=dag,
)


def notify_or_complete_dag(**context):
    """Takes in the latest date for a CF result via XCom and compares with current date.
    As the Airflow context is in UTC and the CF date in Eastern time zone, we need to do
    a bit of conversion to ensure the correct date is looked at. Based on the comparison
    of these dates, we either notify or complete the DAG.

    :param context: Airflow DAG context dict containing information pertaining to DAG run
    :return: task_id of which branch to proceed upon (notify or just complete DAG)
    """
    latest_cf_date = context["task_instance"].xcom_pull(task_ids="get_latest_cf_result")
    try:
        str_date = next(iter(latest_cf_date)).get("latest_date")
        latest_cf_datetime = datetime.strptime(str_date, "%Y-%m-%d")
        if datetime.utcnow().date() > latest_cf_datetime.date():
            return "notify_via_email"
        else:
            return "complete_dag"
    except TypeError as e:
        print(f"Got a type error: {str(e)}; going to complete DAG now")
        return "complete_dag"


notify_or_complete_dag = BranchPythonOperator(
    task_id="notify_or_complete_dag",
    python_callable=notify_or_complete_dag,
    provide_context=True,
    dag=dag,
)

notify_via_email = EmailOperator(
    task_id="notify_via_email",
    to=cf_email,
    subject="Airflow Notification: Computed Field results are stale",
    html_content=(
        "CF data is not up to date (latest is: {{ task_instance.xcom_pull(task_ids='get_latest_cf_result') }} )"
        "\n Please investigate CF job/results"
    ),
    dag=dag,
)

complete_dag = DummyOperator(task_id="complete_dag", dag=dag)

dbt_run_sheets >> dbt_seed_and_snapshot
dbt_seed_and_snapshot >> [dbt_run_nightly, dbt_run_other_computed]
[dbt_run_nightly, prod_mirror_completed] >> dbt_run_commons
dbt_run_commons >> calculate_age_sli
dbt_run_commons >> dbt_test_commons
[dbt_test_commons, dbt_run_other_computed] >> computed_fields_run

# CF alerting
computed_fields_run >> get_latest_cf_result
get_latest_cf_result >> notify_or_complete_dag
notify_or_complete_dag >> [notify_via_email, complete_dag]
