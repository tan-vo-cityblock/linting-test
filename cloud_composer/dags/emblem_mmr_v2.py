import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

job_project = "cityblock-data"
emblem_project = "emblem-data"
mixer_scio_jobs_image = "gcr.io/cityblock-data/mixer_scio_jobs:latest"

secret_mount_emblem = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-load-monthly-emblem",
    key="key.json",
)

dag = DAG(
    dag_id="emblem_mmr_v2",
    start_date=datetime.datetime(2021, 4, 2),  # Data is usually delivered by the 21st.
    schedule_interval="0 0 22 * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

year_month = (datetime.datetime.now()).strftime("%Y%m")
year_month_day = year_month + "01"


scio_load_mmr_emblem = KubernetesPodOperator(
    dag=dag,
    image=mixer_scio_jobs_image,
    namespace="default",
    name="scio-load-mmr-emblem",
    task_id="scio_load_mmr_emblem",
    secrets=[secret_mount_emblem],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        f"--project={job_project}",
        "--runner=DataflowRunner",
        f"--deliveryDate={year_month}",
        f"--tableShardDate={year_month_day}",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=emblem_silver/MMR.txt",
        "--outputDataset=cms_revenue",
        f"--outputProject={emblem_project}",
    ],
)

scio_load_mmr_emblem
