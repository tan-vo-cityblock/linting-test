import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import airflow_utils


""" Runs necessary tasks to load in weekly data provided to CBH by CCI

Schedule: Every day
Workflow: 
  1) find the latest drop date for pharmacy files provided by CCI in our drop bucket, sets this as XCom variable
  2) use this date to check against the latest shards for data loaded into BQ, sets this as XCom variable
  3) if (1) is not found in any relevant shards for partner, we go to (4), otherwise we end here
  4) run Scio jobs to load data into silver
  5) send email to stakeholders that the job is complete

Expectation: CCI delivers this data on a weekly basis, so we expect to execute step 4 and onwards from the 
 workflow once a week.
 Otherwise everyday we will always do 1 -> 2 -> 3 -> end

"""
# *legacy naming*: Amysis used to be the only data vendor that supplied CCI data, so we just called
# it 'cci'
PARTNER = "cci"

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-cci",
    key="key.json",
)

dag = DAG(
    dag_id="load_weekly_data_cci_amisys_v1",
    description="GCS -> BigQuery (silver_claims)",
    start_date=datetime.datetime(2021, 1, 19, 7 + 5),  # Ingest data at 7AM EST
    schedule_interval="0 12 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

get_latest_drop_date = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="get-latest-drop-date",
    task_id="get_latest_drop_date",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=["python", "./get_latest_drop_date_pbm.py", PARTNER],
    do_xcom_push=True,
    dag=dag,
)

bq_shard_exists = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="bq-shard-exists",
    task_id="bq_shard_exists",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./bq_shard_exists.py",
        "--project=connecticare-data",
        "--dataset=silver_claims",
        "--tables=Pharmacy",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    do_xcom_push=True,
    dag=dag,
)


def run_scio_or_end_f(**context):
    found_bq_shard = context["task_instance"].xcom_pull(task_ids="bq_shard_exists")
    if eval(found_bq_shard):
        return "dont_run_scio"
    else:
        return "scio_load_weekly_data"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end_f,
    provide_context=True,
    dag=dag,
)


scio_load_weekly_data = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-weekly-data",
    task_id="scio_load_weekly_data",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--workerMachineType=n1-standard-2",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=cci_silver/Pharmacy.txt",
        "--numWorkers=1",
        "--outputProject=connecticare-data",
    ],
)

update_silver_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-silver-views",
    task_id="update_silver_views",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        "--project=connecticare-data",
        "--dataset=silver_claims",
        "--views=Pharmacy",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

dont_run_scio = DummyOperator(task_id="dont_run_scio", dag=dag)

email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: Load Weekly CCI Pharmacy complete for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content=airflow_utils.bq_result_html(
        "Loading weekly PBM CCI", dag_id=dag.dag_id
    ),
    dag=dag,
)

#  getting latest date and seeing if shard for date exists on relevant data
get_latest_drop_date >> bq_shard_exists

#  using presence of shard to determine whether or not we run scio job
bq_shard_exists >> run_scio_or_end

# branching paths
run_scio_or_end >> scio_load_weekly_data
run_scio_or_end >> dont_run_scio

# alert stakeholders the job is complete
scio_load_weekly_data >> update_silver_views
update_silver_views >> email_stakeholders
