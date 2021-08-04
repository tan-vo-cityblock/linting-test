import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

import airflow_utils

PARTNER = "cci_facets"

PARTNER_PROJECT = "connecticare-data"

silver_tables = [
    "FacetsPharmacy_med",
    "Medical_Facets_med",
    "FacetsProvider",
    "FacetsFacilityProcCodes_med",
]
gold_tables = ["Facility", "Pharmacy", "Professional", "Provider"]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-cci",
    key="key.json",
)

secret_mount_dbt = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)

dag = DAG(
    dag_id="publish_cci_facets_gold_data_v1",
    description="GCS -> BigQuery (gold_claims)",
    start_date=datetime.datetime(2020, 11, 15, 7 + 4),
    schedule_interval=None,
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
    cmds=["python", "./get_latest_drop_date.py", f"--partner={PARTNER}"],
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
        f"--project={PARTNER_PROJECT}",
        "--dataset=gold_claims_facets",
        f'--tables={",".join(gold_tables)}',
        '--date={{ task_instance.xcom_pull(task_ids="get_latest_drop_date") }}',
    ],
    do_xcom_push=True,
    dag=dag,
)


def run_scio_or_end_f(**context):
    found_bq_shard = context["task_instance"].xcom_pull(task_ids="bq_shard_exists")
    if eval(found_bq_shard):
        return "run_scio"
    else:
        return "dont_run_scio"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end_f,
    provide_context=True,
    dag=dag,
)

run_scio = DummyOperator(task_id="run_scio", dag=dag)

dont_run_scio = DummyOperator(task_id="dont_run_scio", dag=dag)

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
        f"--project={PARTNER_PROJECT}",
        "--dataset=silver_claims_facets",
        f'--views={",".join(silver_tables)}',
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",  # jinja template to get latest date
    ],
    dag=dag,
)

update_gold_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-gold-views",
    task_id="update_gold_views",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={PARTNER_PROJECT}",
        "--dataset=gold_claims_facets",
        f'--views={",".join(gold_tables)}',
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",  # jinja template to get latest date
    ],
    dag=dag,
)

push_claims_encounters = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="push-claims-encounters",
    task_id="push_claims_encounters",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PushPatientClaimsEncounters",
        "--environment=prod",
        "--project=cityblock-data",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--runner=DataflowRunner",
        f"--sourceProject={PARTNER_PROJECT}",
        "--sourceDataset=gold_claims_facets",
    ],
    dag=dag,
)

dbt_run_cmd = """	
    dbt deps &&	
    dbt run --model source:cci_facets+ source:cci_facets_silver+ --exclude tag:nightly+	abstractions.computed
"""

dbt_run = KubernetesPodOperator(
    dag=dag,
    get_logs=True,
    secrets=[secret_mount_dbt],
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

email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: CCI Facets data published for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content="Commons patient encounters and dbt tables have been updated with latest data",
    dag=dag,
)

# see if latest drop date matches what's in BQ
get_latest_drop_date >> bq_shard_exists

# using presence of shard to determine whether or not data should be published
bq_shard_exists >> run_scio_or_end

# branching paths
run_scio_or_end >> [run_scio, dont_run_scio]

# update views
run_scio >> [update_silver_views, update_gold_views]

# publish data
update_gold_views >> [push_claims_encounters, dbt_run]

# email stakeholders
[push_claims_encounters, dbt_run] >> email_stakeholders
