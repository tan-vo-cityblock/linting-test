import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

import airflow_utils
from load_partner_data import LoadPartnerDataConfig

""" Runs necessary tasks to load in monthly data dump provided to CBH by Emblem

Schedule: Every day
Workflow:
  1) [get_latest_drop_date] Find the most recent date that Emblem has delivered files within `silver_tables`.
     Return save this date as an XCOM value.
  2) [bq_shard_exists] Check if BigQuery contains shards for `silver_tables` on that date.
     - If shards exist for every table on that date, exit with success.
     - If only some shards exist, immediately fail and log an error.
  3) [scio_load_to_silver] Run LoadToSilver to load each file for the given delivery date to `silver_claims`
     in the `PARTNER`'s project.
  4) [scio_transform_to_gold] Run the PolishEmblemClaimsCohortV2 scio job for the given delivery date.
  5) TODO [flatten_gold] Run flatten_gold.py to populate `gold_claims_flattened` for the given delivery date.
  6) TODO [run_great_expectations] Run `great_expectations` on `gold_claims_flattened` for the given delivery date.
  7) [email_stakeholders] Inform stakeholders that silver_claims, gold_claims, and TODO gold_claims_flattened
  now contain data for a new shard
"""

PARTNER = "emblem_virtual"
GE_PARTNER = "emblem"

bq_job_project = "cityblock-data"

# modify the image tag to pin a version if necessary
load_monthly_data_image = "gcr.io/cityblock-data/load_monthly_data:latest"
mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"
great_expectations_image = "us.gcr.io/cbh-git/great_expectations"

partner_conf = LoadPartnerDataConfig(
    bq_project="emblem-data",
    silver_dataset="silver_claims_virtual",
    gold_dataset="gold_claims_virtual",
    flattened_gold_dataset="gold_claims_virtual_flattened",
)

partner_config_bucket = "cbh-partner-configs"
silver_tables = [
    "diagnosis_associations",
    "facility",
    "member_demographics",
    "member_month",
    "pharmacy",
    "procedure_associations",
    "professional",
    "providers",
    "lab_results",
    "member_enroll",
    "employer_group",
]
silver_config_paths = [f"emblem_silver/{table}_virtual.txt" for table in silver_tables]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-emblem",
    key="key.json",
)

ge_slack_webhook_env = secret.Secret(
    deploy_type="env",
    deploy_target="SLACK_WEBHOOK",
    secret="prod-ge-slack-webhook",
    key="latest",
)

dag = DAG(
    dag_id="load_monthly_virtual_data_emblem_v1",
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
        f'--tables={",".join(silver_tables)}',
        '--date={{ task_instance.xcom_pull(task_ids="get_latest_drop_date") }}',
    ],
    do_xcom_push=True,
    dag=dag,
)


def run_scio_or_end_f(**context):
    found_bq_shard = context["task_instance"].xcom_pull(task_ids="bq_shard_exists")
    if eval(found_bq_shard):
        return "dont_run_scio"
    else:
        return "run_scio"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end_f,
    provide_context=True,
    dag=dag,
)

run_scio = DummyOperator(task_id="run_scio", dag=dag)

dont_run_scio = DummyOperator(task_id="dont_run_scio", dag=dag)

scio_load_crosswalk = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-parse-crosswalk",
    task_id="scio_parse_crosswalk",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        f"--outputProject={partner_conf.bq_project}",
        f"--outputDataset={partner_conf.silver_dataset}",
        "--tableShardDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        # crosswalk file names format dates like yyyyMM
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date')[0:6] }}",
        f"--inputConfigBucket={partner_config_bucket}",
        "--inputConfigPaths=emblem_silver/crosswalk_virtual.txt",
    ],
    dag=dag,
)

scio_load_to_silver = KubernetesPodOperator(
    dag=dag,
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
        f"--outputProject={partner_conf.bq_project}",
        f"--outputDataset={partner_conf.silver_dataset}",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--inputConfigBucket={partner_config_bucket}",
        f'--inputConfigPaths={",".join(silver_config_paths)}',
        "--experiments=upload_graph",
    ],
)

update_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-emblem-digital-monthly-silver-view",
    task_id="update_emblem_digital_monthly_silver_view",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={partner_conf.bq_project}",
        f"--dataset={partner_conf.silver_dataset}",
        "--views=member_enroll,member_demographics,member_month",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

scio_transform_to_gold = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-polish-data",
    task_id="scio_polish_data",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishEmblemClaimsDataV2",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--sourceProject={partner_conf.bq_project}",
        f"--sourceDataset={partner_conf.silver_dataset}",
        f"--destinationProject={partner_conf.bq_project}",
        f"--destinationDataset={partner_conf.gold_dataset}",
    ],
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
        "--tables=Professional,Facility,Pharmacy",
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
        f"--partner={GE_PARTNER}",
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
        f"--partner={GE_PARTNER}",
        f"--bq_table={partner_conf.bq_project}.{partner_conf.flattened_gold_dataset}.Professional_"
        + "{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--run_name=airflow_{dag.dag_id}_flat_gold_professional",
    ],
    dag=dag,
)

run_great_expectations_lab = KubernetesPodOperator(
    image=great_expectations_image,
    namespace="default",
    name="run-great-expectations-lab",
    task_id="run_great_expectations_lab",
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
        "--stage=gold",
        "--data_group=labs",
        f"--partner={GE_PARTNER}",
        f"--bq_table={partner_conf.bq_project}.{partner_conf.gold_dataset}.LabResult_"
        + "{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        f"--run_name=airflow_{dag.dag_id}_gold_lab",
    ],
    dag=dag,
)

email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: Load monthly virtual Emblem complete for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content=partner_conf.result_email_html(
        description="Load monthly virtual Emblem",
        dag_id=dag.dag_id,
        shard="{{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ),
    dag=dag,
)

#  getting latest date and seeing if shard for date exists on relevant data
get_latest_drop_date >> bq_shard_exists

#  using presence of shard to determine whether or not we run scio job
bq_shard_exists >> run_scio_or_end

# branching paths
run_scio_or_end >> [run_scio, dont_run_scio]


# loading the crosswalk doesn't block the other load/transform tasks (scio_load_crosswalk is only a
# separate step because the crosswalk has a different date format)
run_scio >> scio_load_crosswalk

# gcs -> silver -> gold
run_scio >> scio_load_to_silver
scio_load_to_silver >> update_views
update_views >> scio_transform_to_gold

# wait for load jobs to finish before e-mailing stakeholders and QAing lab results
scio_transform_to_gold >> [email_stakeholders, run_great_expectations_lab, flatten_gold]

# flatten -> QA
flatten_gold >> [run_great_expectations_facility, run_great_expectations_professional]
