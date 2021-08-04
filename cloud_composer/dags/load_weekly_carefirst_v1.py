import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

import airflow_utils
from load_partner_data import LoadPartnerDataConfig

load_data_image = "gcr.io/cityblock-data/load_monthly_data:latest"
mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"
great_expectations_image = "us.gcr.io/cbh-git/great_expectations"

partner_conf = LoadPartnerDataConfig(
    bq_project="cbh-carefirst-data",
    silver_dataset="silver_claims",
    gold_dataset="gold_claims_incremental",
    combined_gold_dataset="gold_claims",
    flattened_gold_dataset="gold_claims_flattened",
)

silver_tables = [
    "facility",
    "professional",
    "diagnosis_association",
    "procedure_association",
    "pharmacy",
    "lab_result",
    "provider",
]
silver_config_paths = [f"carefirst_silver/{table}.txt" for table in silver_tables]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-carefirst-worker",
    key="key.json",
)

dag = DAG(
    dag_id="load_weekly_carefirst_v1",
    description="Weekly Carefirst data: facility, professional, diagnosis, procedure, provider and pharmacy",
    start_date=datetime.datetime(2021, 1, 19, 12 + 5),  # Ingest data at 12pm EST.
    schedule_interval="0 17 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

get_latest_drop_date = KubernetesPodOperator(
    image=load_data_image,
    namespace="default",
    name="get-latest-drop-date",
    task_id="get_latest_drop_date",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=["python", "./get_latest_drop_date.py", "--partner=carefirst_weekly"],
    do_xcom_push=True,
    dag=dag,
)

bq_shard_exists = KubernetesPodOperator(
    image=load_data_image,
    namespace="default",
    name="bq-shard-exists",
    task_id="bq_shard_exists",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./bq_shard_exists.py",
        f"--project=cbh-carefirst-data",
        f"--dataset=silver_claims",
        f"--tables={','.join(silver_tables)}",
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
        return "scio_load_to_silver"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end_f,
    provide_context=True,
    dag=dag,
)

dont_run_scio = DummyOperator(task_id="dont_run_scio", dag=dag)

scio_load_to_silver = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-load-to-silver",
    task_id="scio_load_to_silver",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--inputConfigBucket=cbh-partner-configs",
        f'--inputConfigPaths={",".join(silver_config_paths)}',
        f"--outputProject=cbh-carefirst-data",
        f"--outputDataset=silver_claims",
    ],
    dag=dag,
)

scio_transform_to_gold_incremental = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-transform-to-gold-incremental",
    task_id="scio_transform_to_gold_incremental",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishCareFirstWeeklyData",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--sourceProject=cbh-carefirst-data",
        "--sourceDataset=silver_claims",
        "--destinationProject=cbh-carefirst-data",
        "--destinationDataset=gold_claims_incremental",
    ],
    dag=dag,
)

scio_transform_to_gold = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-transform-to-gold",
    task_id="scio_transform_to_gold",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishCarefirstFullRefreshClaims",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--sourceProject=cbh-carefirst-data",
        "--sourceDataset=silver_claims",
        "--destinationProject=cbh-carefirst-data",
        "--destinationDataset=gold_claims",
    ],
    dag=dag,
)

bq_get_latest_combined_gold_shard = KubernetesPodOperator(
    image=load_data_image,
    namespace="default",
    name="bq-get-latest-combined-gold-shard",
    task_id="bq_get_latest_combined_gold_shard",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./get_latest_bq_table_shard.py",
        f"--project={partner_conf.bq_project}",
        "--dataset=gold_claims",
        "--required_tables=Facility,LabResult,Pharmacy,Professional",
    ],
    do_xcom_push=True,
    dag=dag,
)

scio_combine_gold = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-combine-gold",
    task_id="scio_combine_gold",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/CombineReplaceData",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--newDeliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--oldDeliveryDate={{ task_instance.xcom_pull(task_ids='bq_get_latest_combined_gold_shard') }}",
        f"--sourceProject={partner_conf.bq_project}",
        "--tablesToCombine=Professional,Facility,Pharmacy,LabResult"
    ],
    dag=dag,
)

email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: Weekly CareFirst data loaded for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content=airflow_utils.bq_result_html(
        "Loading weekly CareFirst data", dag_id=dag.dag_id
    ),
    dag=dag,
)

# getting latest date and seeing if shard for date exists on relevant data
get_latest_drop_date >> bq_shard_exists

# using presence of shard to determine whether or not we run scio job
bq_shard_exists >> run_scio_or_end

# branching paths
run_scio_or_end >> dont_run_scio
run_scio_or_end >> scio_load_to_silver

# running polish and combine jobs after loading in data
scio_load_to_silver >> [scio_transform_to_gold_incremental, scio_transform_to_gold]
scio_transform_to_gold_incremental >> bq_get_latest_combined_gold_shard
bq_get_latest_combined_gold_shard >> scio_combine_gold

# notification e-mail
[scio_combine_gold, scio_transform_to_gold] >> email_stakeholders
