import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

import airflow_utils
from load_partner_data import LoadPartnerDataConfig

""" Runs necessary tasks to load in monthly data dump provided to CBH by Tufts

Schedule: Every day
Workflow:
  1) [get_latest_drop_date] Find the most recent date that Tufts has delivered files within `silver_tables`.
     Return save this date as an XCOM value.
  2) [bq_shard_exists] Check if BigQuery contains shards for `silver_tables` on that date.
     - If shards exist for every table on that date, exit with success.
     - If only some shards exist, immediately fail and log an error.
  3) [scio_load_to_silver] Run LoadToSilver to load each file for the given delivery date to `silver_claims`
     in the tufts-data project.
  4a) [scio_transform_to_gold_claims] Run PolishTuftsData scio job for the given delivery date and
     indicate that Professional, Facility, and Pharamcy should go to gold_claims_incremental
  4b) [scio_transform_to_gold_provider] Run PolishTuftsData scio job for the given delivery date and
     indicate that Provider should go to gold_claims.
  5) [bq_get_latest_combined_gold_shard] Find the most recent shard within gold_claims that contains
     a Facility, Professional, and Pharmacy table.
  6) [scio_combine_gold] Run `CombineTuftsData` to add the changes from the just-loaded shard to
      the shard returned by `bq_bq_get_latest_combined_gold_shard`.
  7) [flatten_gold] Run `dbt` to populate `gold_claims_flattened` from the result of scio_combine_gold.
  8) [run_great_expectations] Run `great_expectations` on `gold_claims_flattened` for the given delivery date.
  9) [email_stakeholders] Inform stakeholders that silver_claims, gold_claims_incremental, and gold_claims. 

Expectations: 
- All files from `silver_tables` delivered on a monthly basis
- For a given delivery, every filename is suffixed with the same datestamp
"""

PARTNER = "tufts"

# modify the image tag to pin a version if necessary
load_monthly_data_image = "gcr.io/cityblock-data/load_monthly_data:latest"
mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"
great_expectations_image = "us.gcr.io/cbh-git/great_expectations"

bq_job_project = "cityblock-data"

partner_conf = LoadPartnerDataConfig(
    bq_project="tufts-data",
    silver_dataset="silver_claims",
    gold_dataset="gold_claims_incremental",
    combined_gold_dataset="gold_claims",
)

partner_config_bucket = "cbh-partner-configs"
silver_tables = ["Provider", "PharmacyClaim", "MedicalClaim", "Member"]
silver_config_paths = [f"tufts_silver/{table}.txt" for table in silver_tables]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-daily-tufts",
    key="key.json",
)

ge_slack_webhook_env = secret.Secret(
    deploy_type="env",
    deploy_target="SLACK_WEBHOOK",
    secret="prod-ge-slack-webhook",
    key="latest",
)

dag = DAG(
    dag_id="load_monthly_data_tufts_v1",
    description="GCS -> BigQuery (gold_claims)",
    start_date=datetime.datetime(2021, 1, 19, 7 + 5),
    schedule_interval="0 12 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

get_latest_drop_date = KubernetesPodOperator(
    image=load_monthly_data_image,
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
    image=load_monthly_data_image,
    namespace="default",
    name="bq-shard-exists",
    task_id="bq_shard_exists",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./bq_shard_exists.py",
        f"--project={partner_conf.bq_project}",
        f"--dataset={partner_conf.silver_dataset}",
        f"--tables={','.join(silver_tables)}",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    do_xcom_push=True,
    dag=dag,
)


def run_scio_or_end(**context):
    found_bq_shard = context["task_instance"].xcom_pull(task_ids="bq_shard_exists")
    if eval(found_bq_shard):
        return "dont_run_scio"
    else:
        return "scio_load_to_silver"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end,
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
        f"--project={bq_job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--inputConfigBucket={partner_config_bucket}",
        f'--inputConfigPaths={",".join(silver_config_paths)}',
        f"--outputProject={partner_conf.bq_project}",
        f"--outputDataset={partner_conf.silver_dataset}",
    ],
    dag=dag,
)

update_silver_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-tufts-monthly-silver-view",
    task_id="update_tufts_monthly_silver_view",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={partner_conf.bq_project}",
        f"--dataset={partner_conf.silver_dataset}",
        "--views=Provider",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

scio_transform_to_gold_rosters = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-transform-to-gold-provider",
    task_id="scio_transform_to_gold_provider",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishTuftsData",
        "--environment=prod",
        f"--project={bq_job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--sourceProject={partner_conf.bq_project}",
        f"--sourceDataset={partner_conf.silver_dataset}",
        f"--destinationProject={partner_conf.bq_project}",
        "--destinationDataset=gold_claims",
        "--goldTables=Provider,Member",
    ],
    dag=dag,
)

scio_transform_to_gold_claims_incrementals = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-transform-to-gold-claims",
    task_id="scio_transform_to_gold_claims",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishTuftsData",
        "--environment=prod",
        f"--project={bq_job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--sourceProject={partner_conf.bq_project}",
        f"--sourceDataset={partner_conf.silver_dataset}",
        f"--destinationProject={partner_conf.bq_project}",
        f"--destinationDataset={partner_conf.gold_dataset}",
        "--goldTables=Professional,Facility,Pharmacy",
    ],
    dag=dag,
)

bq_get_latest_combined_gold_shard = KubernetesPodOperator(
    image=load_monthly_data_image,
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
        "--required_tables=Professional,Facility,Pharmacy",
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
        "bin/CombineTuftsData",
        "--environment=prod",
        f"--project={bq_job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--previousDeliveryDate={{ task_instance.xcom_pull(task_ids='bq_get_latest_combined_gold_shard') }}",
        f"--sourceNewProject={partner_conf.bq_project}",
        f"--sourceNewDataset={partner_conf.gold_dataset}",
        f"--sourceOldProject={partner_conf.bq_project}",
        f"--sourceOldDataset={partner_conf.combined_gold_dataset}",
        f"--destinationProject={partner_conf.bq_project}",
        f"--destinationDataset={partner_conf.combined_gold_dataset}",
    ],
    dag=dag,
)

update_gold_tufts_masshealth_prof_fac_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-gold-tufts-masshealth-prof-fac-views",
    task_id="update_gold_tufts_masshealth_prof_fac_views",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=["python", "./masshealth_combine.py", "--update_views_only"],
    dag=dag,
)


flatten_gold = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="flatten-gold",
    task_id="flatten_gold",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./flatten_gold.py",
        f"--partner={PARTNER}",
        f"--project={bq_job_project}",
        f"--input_project={partner_conf.bq_project}",
        f"--input_dataset={partner_conf.gold_dataset}",
        f"--output_project={partner_conf.bq_project}",
        f"--output_dataset={partner_conf.flattened_gold_dataset}",
        "--shard={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--tables=Professional,Facility,Pharmacy",
    ],
    dag=dag,
)


run_great_expectations_facility = KubernetesPodOperator(
    image=great_expectations_image,
    namespace="default",
    name="run-great-expectations-facility",
    task_id="run_great_expectations_facility",
    secrets=[secret_mount, ge_slack_webhook_env],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
        "GOOGLE_CLOUD_PROJECT": bq_job_project,
    },
    image_pull_policy="Always",
    cmds=[
        "python",
        "src/main.py",
        "--env=prod",
        "--validate",
        "--stage=flat_gold",
        "--data_group=facility",
        f"--partner={PARTNER}",
        f"--bq_table={partner_conf.bq_project}.{partner_conf.flattened_gold_dataset}.Facility_"
        + "{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--run_name=airflow_{dag.dag_id}_flat_gold_facility",
    ],
    dag=dag,
)


run_great_expectations_professional = KubernetesPodOperator(
    image=great_expectations_image,
    namespace="default",
    name="run-great-expectations-professional",
    task_id="run_great_expectations_professional",
    secrets=[secret_mount, ge_slack_webhook_env],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
        "GOOGLE_CLOUD_PROJECT": bq_job_project,
    },
    image_pull_policy="Always",
    cmds=[
        "python",
        "src/main.py",
        "--env=prod",
        "--validate",
        "--stage=flat_gold",
        "--data_group=professional",
        f"--partner={PARTNER}",
        f"--bq_table={partner_conf.bq_project}.{partner_conf.flattened_gold_dataset}.Professional_"
        + "{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--run_name=airflow_{dag.dag_id}_flat_gold_professional",
    ],
    dag=dag,
)


email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: Load Monthly Tufts complete for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content=partner_conf.result_email_html(
        description="Load monthly Tufts",
        dag_id=dag.dag_id,
        shard="{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
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

scio_load_to_silver >> update_silver_views
update_silver_views >> [
    scio_transform_to_gold_claims_incrementals,
    scio_transform_to_gold_rosters,
]

scio_transform_to_gold_claims_incrementals >> [
    bq_get_latest_combined_gold_shard,
    email_stakeholders,
]
bq_get_latest_combined_gold_shard >> scio_combine_gold
scio_combine_gold >> update_gold_tufts_masshealth_prof_fac_views
scio_combine_gold >> flatten_gold

# qa steps
flatten_gold >> [run_great_expectations_facility, run_great_expectations_professional]