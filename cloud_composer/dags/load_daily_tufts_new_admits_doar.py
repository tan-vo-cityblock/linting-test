import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow_utils import default_args

secret_mount = secret.Secret(
    deploy_type='volume',
    deploy_target='/var/secrets/google',
    secret='tf-svc-load-daily-tufts',
    key='key.json'
)

dag = DAG(
    dag_id='load_daily_tufts_new_admits_doar',
    description="Daily Tufts New Admits and DOAR data: Ingestion",
    start_date=datetime.datetime(2021, 8, 1, 9 + 5), # Ingest data at 9am EST.
    schedule_interval='0 14 * * *',
    default_args=default_args,
    catchup=False
)

scio_load_daily_tufts_new_admits_doar = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-daily-tufts-new-admits-doar",
    task_id="scio_load_daily_tufts_new_admits_doar",
    secrets=[secret_mount],
    env_vars={'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/key.json'},
    image_pull_policy="Always",
    cmds=[
        'bin/LoadToSilverRunner',
        '--environment=prod',
        '--project=cityblock-data',
        '--runner=DataflowRunner',
        "--tableShardDate={{ next_ds_nodash }}",
        '--deliveryDate={{ macros.ds_format(next_ds_nodash, "%Y%m%d", "%m%d%Y") }}',
        '--inputConfigBucket=cbh-partner-configs',
        '--inputConfigPaths=tufts_silver/DOAR.txt,tufts_silver/NewAdmits.txt',
        '--outputProject=tufts-data'
    ]
)

update_silver_views_daily_new_admits_doar_tufts = KubernetesPodOperator(
    dag=dag,
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    namespace="default",
    name="update-silver-views-daily-new-admits-doar-tufts",
    task_id="update_silver_views_daily_new_admits_doar_tufts",
    secrets=[secret_mount],
    env_vars={'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/key.json'},
    image_pull_policy="Always",
    cmds=[
        'python',
        './update_views.py',
        '--project=tufts-data',
        '--dataset=silver_claims',
        '--views=DOAR,NewAdmits',
        "--date={{ next_ds_nodash }}"
    ]
)

scio_load_daily_tufts_new_admits_doar >> update_silver_views_daily_new_admits_doar_tufts
