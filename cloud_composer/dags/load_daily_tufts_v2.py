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
    dag_id="load_daily_tufts_v2",
    description="Tufts Daily Membership Files: raw to gold",
    start_date=datetime.datetime(
        2021, 1, 19, 9 + 5
    ),  # Process files at 9am EST, after daily job & monthly
    schedule_interval="0 14 * * *",
    default_args=default_args,
    catchup=True,
)

scio_load_daily_data_tufts = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-daily-data-tufts",
    task_id="scio_load_daily_data_tufts",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ next_ds_nodash }}",
        "--workerMachineType=n2-highcpu-2",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=tufts_silver/MemberDaily.txt",
        "--numWorkers=2",
        "--outputProject=tufts-data",
    ],
    dag=dag,
)

update_views = KubernetesPodOperator(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-tufts-daily-silver-view",
    task_id="update_tufts_daily_silver_view",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./update_views.py",
        "--project=tufts-data",
        "--dataset=silver_claims",
        "--views=Member_Daily",
        "--date={{ next_ds_nodash }}",
    ],
    dag=dag,
)

scio_polish_data_tufts_daily = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-polish-data-tufts-daily",
    task_id="scio_polish_data_tufts_daily",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/PolishTuftsDataDaily",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        "--deliveryDate={{ next_ds_nodash }}",
        "--workerMachineType=n2-highcpu-2",
        "--numWorkers=2",
        "--destinationProject=tufts-data",
    ],
    dag=dag,
)

dbt_run_cmd = """
    dbt deps &&
    dbt run -m source:tufts.Member_Daily_*+ --exclude source:commons+ tag:nightly+
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

dbt_test_cmd = """
    dbt deps &&
    dbt test -m source:tufts.Member_Daily_*+ --exclude source:commons+ tag:nightly+
"""

dbt_test = KubernetesPodOperator(
    dag=dag,
    get_logs=True,
    secrets=[secret_mount_dbt],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
        "DBT_PROFILES_DIR": "/profiles",
    },
    task_id="dbt_test",
    name="dbt-test",
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_test_cmd],
    namespace="default",
    image="gcr.io/cityblock-orchestration/dbt:latest",
    image_pull_policy="Always",
)

scio_load_daily_data_tufts >> update_views
update_views >> scio_polish_data_tufts_daily
scio_polish_data_tufts_daily >> dbt_run
dbt_run >> dbt_test
