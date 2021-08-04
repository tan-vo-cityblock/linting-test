import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret

from airflow_utils import default_args

mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"
load_monthly_data_image = "gcr.io/cityblock-data/load_monthly_data:latest"

job_project = "cityblock-data"
emblem_project = "emblem-data"
connecticare_project = "connecticare-data"

secret_mount_emblem = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-emblem",
    key="key.json",
)

secret_mount_connecticare = secret.Secret(
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
    dag_id="load_daily_emblem_connecticare_prior_auth_v2",
    description="Daily Emblem and ConnectiCare Prior Authorization data: Ingestion and processing",
    start_date=datetime.datetime(2021, 1, 19, 6 + 5),  # Ingest data at 6am EST.
    schedule_interval="0 11 * * *",
    default_args=default_args,
    catchup=False,
)

scio_load_daily_prior_auth_emblem = KubernetesPodOperator(
    dag=dag,
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-load-daily-prior-auth-emblem",
    task_id="scio_load_daily_prior_auth_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        f"--project={job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ next_ds_nodash }}",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=emblem_silver/prior_authorization.txt",
        f"--outputProject={emblem_project}",
    ],
)


scio_load_daily_prior_auth_connecticare = KubernetesPodOperator(
    dag=dag,
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-load-daily-prior-auth-connecticare",
    task_id="scio_load_daily_prior_auth_connecticare",
    secrets=[secret_mount_connecticare],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        f"--project={job_project}",
        "--runner=DataflowRunner",
        "--deliveryDate={{ next_ds_nodash }}",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=cci_silver/prior_authorization.txt",
        f"--outputProject={connecticare_project}",
    ],
)


update_prior_auth_views_silver_emblem = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="update-prior-auth-views-silver-emblem",
    task_id="update_prior_auth_views_silver_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={emblem_project}",
        "--dataset=silver_claims",
        "--views=prior_authorization",
        "--date={{ next_ds_nodash }}",
    ],
    dag=dag,
)


update_prior_auth_views_silver_connecticare = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="update-prior-auth-views-silver-connecticare",
    task_id="update_prior_auth_views_silver_connecticare",
    secrets=[secret_mount_connecticare],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={connecticare_project}",
        "--dataset=silver_claims",
        "--views=prior_authorization",
        "--date={{ next_ds_nodash }}",
    ],
    dag=dag,
)

bq_get_latest_gold_shard_emblem = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="bq-get-latest-gold-shard-emblem",
    task_id="bq_get_latest_gold_shard_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./get_latest_bq_table_shard.py",
        f"--project={emblem_project}",
        "--dataset=gold_claims",
        "--required_tables=PriorAuthorization",
    ],
    do_xcom_push=True,
    dag=dag,
)


transform_prior_auth_gold_emblem = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="transform-prior-auth-gold-emblem",
    task_id="transform_prior_auth_gold_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PriorAuthToHIETransformer",
        "--runner=DataflowRunner",
        f"--project={job_project}",
        "--deliveryDate={{ next_ds_nodash }}",
        f"--bigqueryProject={emblem_project}",
        "--previousDeliveryDate={{ task_instance.xcom_pull(task_ids='bq_get_latest_gold_shard_emblem') }}",
        "--sendToCommons=true",
        "--source=Emblem",
    ],
    dag=dag,
)


transform_prior_auth_gold_connecticare = KubernetesPodOperator(
    image=mixer_scio_jobs_image,
    namespace="default",
    name="transform-prior-auth-gold-connecticare",
    task_id="transform_prior_auth_gold_connecticare",
    secrets=[secret_mount_connecticare],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PriorAuthToHIETransformer",
        "--runner=DataflowRunner",
        f"--project={job_project}",
        "--deliveryDate={{ next_ds_nodash }}",
        f"--bigqueryProject={connecticare_project}",
        "--sendToCommons=false",
    ],
    dag=dag,
)


update_prior_auth_views_gold_emblem = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="update-prior-auth-views-gold-emblem",
    task_id="update_prior_auth_views_gold_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={emblem_project}",
        "--dataset=gold_claims",
        "--views=PriorAuthorization",
        "--date={{ next_ds_nodash }}",
    ],
    dag=dag,
)


update_prior_auth_views_gold_connecticare = KubernetesPodOperator(
    image=load_monthly_data_image,
    namespace="default",
    name="update-prior-auth-views-gold-connecticare",
    task_id="update_prior_auth_views_gold_connecticare",
    secrets=[secret_mount_connecticare],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={connecticare_project}",
        "--dataset=gold_claims",
        "--views=PriorAuthorization",
        "--date={{ next_ds_nodash }}",
    ],
    dag=dag,
)


dbt_run_cmd = """
    dbt deps &&
    dbt run -m source:cci.PriorAuthorization_*+4 source:emblem.PriorAuthorization_*+4
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


scio_load_daily_prior_auth_emblem >> update_prior_auth_views_silver_emblem
update_prior_auth_views_silver_emblem >> bq_get_latest_gold_shard_emblem
bq_get_latest_gold_shard_emblem >> transform_prior_auth_gold_emblem
transform_prior_auth_gold_emblem >> update_prior_auth_views_gold_emblem

scio_load_daily_prior_auth_connecticare >> update_prior_auth_views_silver_connecticare
update_prior_auth_views_silver_connecticare >> transform_prior_auth_gold_connecticare
transform_prior_auth_gold_connecticare >> update_prior_auth_views_gold_connecticare

[
    update_prior_auth_views_gold_emblem,
    update_prior_auth_views_gold_connecticare,
] >> dbt_run
