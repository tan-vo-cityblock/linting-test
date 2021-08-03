import datetime

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

import airflow_utils

""" Runs necessary tasks to load in monthly data dump provided to CBH by Connecticare Facets

Schedule: Every day
Workflow:
  1) [get_latest_drop_date] Find the most recent date that CCI Facets has delivered files within `silver_tables`.
     Return save this date as an XCOM value.
  2) [bq_silver_shard_exists] Check if BigQuery contains shards for `silver_tables` on that date.
     - If shards exist for every table on that date, exit with success.
  3) [split_facets_member_file] Separate 'FacetsMember_med' into three separate files (based on the
     prefix of each line) and store the new files in `FACETS_WRITE_BUCKET`.
  4) [scio_load_to_silver] Run LoadToSilver to load each file for the given delivery date to `silver_claims`
     in the partner's project.
  5) [email_stakeholders] Inform stakeholders that silver_claims_facets now contains data for a new shard

Expectations: 
- Weekly delivery
- For a given delivery, every filename is suffixed with the same datestamp
"""

# modify the image tag to pin a version if necessary
load_monthly_data_image = "gcr.io/cityblock-data/load_monthly_data:latest"
mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"

silver_tables = [
    "Attributed_PCP_med",
    "Member_Facets_med",
    "Health_Coverage_Benefit_med",
]
silver_config_paths = [f"cci_silver/{table}.txt" for table in silver_tables]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-cci",
    key="key.json",
)

dag = DAG(
    dag_id="load_weekly_data_cci_facets_v1",
    description="GCS -> BigQuery (silver_claims)",
    start_date=datetime.datetime(2021, 1, 19, 7 + 5),  # Ingest data at 7AM EST
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
    cmds=["python", "./get_latest_drop_date.py", "--partner=cci_facets_weekly"],
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
        f"--project=connecticare-data",
        f"--dataset=silver_claims_facets",
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
        return "split_facets_member_file"


run_scio_or_end = BranchPythonOperator(
    task_id="run_scio_or_end",
    python_callable=run_scio_or_end_f,
    provide_context=True,
    dag=dag,
)

dont_run_scio = DummyOperator(task_id="dont_run_scio", dag=dag)

split_facets_member_file = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="split-facets-member-file",
    task_id="split_facets_member_file",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./split_facets_member_file.py",
        f"--bucket=cbh-raw-transforms",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

scio_load_to_silver = KubernetesPodOperator(
    dag=dag,
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-load-weekly-data-cci-facets",
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
        "--inputConfigBucket=cbh-partner-configs",
        f'--inputConfigPaths={",".join(silver_config_paths)}',
        "--outputProject=connecticare-data",
        "--outputDataset=silver_claims_facets",
    ],
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
        "bin/PolishCCIFacetsWeeklyData",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
        "--sourceProject=connecticare-data",
        "--sourceDataset=silver_claims_facets",
        "--destinationProject=connecticare-data",
        "--destinationDataset=gold_claims_facets",
    ],
    dag=dag,
)

update_silver_views = KubernetesPodOperator(
    image=load_monthly_data_image,
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
        "--dataset=silver_claims_facets",
        f'--views={",".join(silver_tables)}',
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

update_gold_views = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="update-gold-views",
    task_id="update_gold_views",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        "--project=connecticare-data",
        "--dataset=gold_claims_facets",
        f"--views=Member",
        "--date={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    ],
    dag=dag,
)

email_stakeholders = EmailOperator(
    task_id="email_stakeholders",
    to=airflow_utils.data_group,
    subject="Airflow: Weekly CCI Facets eligibility data loaded for shard: {{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",
    html_content=airflow_utils.bq_result_html(
        "Loading weekly CCI data", dag_id=dag.dag_id
    ),
    dag=dag,
)

publish_member_attribution = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="publish-member-attribution",
    task_id="publish_member_attribution",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PublishMemberAttributionData",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ task_instance.xcom_pull(task_ids='get_latest_drop_date') }}",  # jinja template to get latest date
        "--sourceProject=connecticareproduction",
        "--bigqueryProject=connecticare-data" "--destinationProject=cityblock-data",
        "--dataset=gold_claims_facets" "--publish=true",
    ],
    dag=dag,
)

# getting latest date and seeing if shard for date exists on relevant data
get_latest_drop_date >> bq_shard_exists

# using presence of shard to determine whether or not we run scio job
bq_shard_exists >> run_scio_or_end

# branching paths
run_scio_or_end >> dont_run_scio
run_scio_or_end >> split_facets_member_file

# load in silver data
split_facets_member_file >> scio_load_to_silver

# running polish jobs after loading in data
scio_load_to_silver >> scio_transform_to_gold

# update views
scio_load_to_silver >> update_silver_views
scio_transform_to_gold >> update_gold_views
update_gold_views >> publish_member_attribution

# notification e-mail
publish_member_attribution >> email_stakeholders
