import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow_utils import default_args

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-daily-tufts",
    key="key.json",
)

secret_mount_dbt = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)

dag = DAG(
    dag_id="load_daily_tufts_prior_auth_v2",
    description="Daily Tufts Prior Authorization data: Ingestion and processing",
    start_date=datetime.datetime(2021, 1, 19, 9 + 5),  # Ingest data at 9am EST.
    schedule_interval="0 14 * * *",
    default_args=default_args,
    catchup=False,
)

scio_load_daily_prior_auth_tufts = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-daily-prior-auth-tufts",
    task_id="scio_load_daily_prior_auth_tufts",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--tableShardDate={{ next_ds_nodash }}",
        '--deliveryDate={{ macros.ds_format(next_ds_nodash, "%Y%m%d", "%m%d%Y") }}',
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=tufts_silver/PriorAuthorizationAdmission.txt,tufts_silver/PriorAuthorizationReferral.txt",
        "--outputProject=tufts-data",
    ],
)

update_silver_views_daily_prior_auth_tufts = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-silver-views-daily-prior-auth-tufts",
    task_id="update_silver_views_daily_prior_auth_tufts",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        "--project=tufts-data",
        "--dataset=silver_claims",
        "--views=PriorAuthorizationAdmission,PriorAuthorizationReferral",
        "--date={{ next_ds_nodash }}",
    ],
)

dbt_run_cmd = """
    dbt deps &&
    dbt run -m source:tufts_silver.PriorAuthorizationAdmission_*+ source:tufts_silver.PriorAuthorizationReferral_*+ &&
    dbt test -m source:tufts_silver.PriorAuthorizationAdmission_*+ source:tufts_silver.PriorAuthorizationReferral_*+
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

scio_load_daily_prior_auth_tufts >> update_silver_views_daily_prior_auth_tufts
update_silver_views_daily_prior_auth_tufts >> dbt_run
