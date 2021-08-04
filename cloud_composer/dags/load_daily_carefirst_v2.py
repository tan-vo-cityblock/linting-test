from collections import namedtuple
import datetime
import re
from typing import Dict, List, Optional
from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret
from airflow_utils import default_args
from airflow.operators.python_operator import PythonOperator


def xcom_pull(key: str, task_id: str):
    return f"{{{{task_instance.xcom_pull(key='{key}', task_ids='{task_id}')}}}}"


PAYER_PROJECT = "cbh-carefirst-data"

config_prefix = "carefirst_silver/"
configs = [
    {
        "nickname": nickname,
        "name": f"member_{nickname}",
        "filename": f"member_{nickname}.txt",
        "filepath": f"{config_prefix}member_{nickname}.txt",
    }
    for nickname in ["demographics", "month"]
]

secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-carefirst-worker",
    key="key.json",
)

dag = DAG(
    dag_id="load_daily_carefirst_v2",
    description="Daily Carefirst Member Demographics and Insurance Extract Information",
    start_date=datetime.datetime(2021, 5, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


def KubernetesStep(image: str, task_id: str, cmds: List[str]):
    return KubernetesPodOperator(
        dag=dag,
        image=image,
        namespace="default",
        name=task_id.replace("_", "-"),
        task_id=task_id,
        secrets=[secret_mount],
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
        image_pull_policy="Always",
        cmds=cmds,
    )


def LoadToSilverStep(
    task_id: str, delivery_date: str, input_config_paths: str, output_project: str
):
    return KubernetesStep(
        image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
        task_id=task_id,
        cmds=[
            "bin/LoadToSilverRunner",
            "--environment=prod",
            "--project=cityblock-data",
            "--runner=DataflowRunner",
            f"--deliveryDate={delivery_date}",
            "--inputConfigBucket=cbh-partner-configs",
            f"--inputConfigPaths={input_config_paths}",
            f"--outputProject={output_project}",
        ],
    )


def parse_config_to_xcom(**context):
    # Expected filename example: 20210119
    context["task_instance"].xcom_push(
        key="date", value=context["dag_run"].conf["carefirstMemberDailyDate"]
    )


config_to_xcom = PythonOperator(
    task_id="config_to_xcom",
    python_callable=parse_config_to_xcom,
    provide_context=True,
    dag=dag,
)

scio_load_daily_demographics_insurance_extract = LoadToSilverStep(
    task_id=f"scio_load_daily_carefirst_insurance_extract",
    delivery_date=xcom_pull("date", config_to_xcom.task_id),
    input_config_paths=",".join([config["filepath"] for config in configs]),
    output_project=PAYER_PROJECT,
)

update_views_for_demographics_and_member_month = KubernetesStep(
    image="gcr.io/cityblock-data/load_monthly_data:latest",
    task_id="update_carefirst_daily_silver_view",
    cmds=[
        "python",
        "./update_views.py",
        f"--project={PAYER_PROJECT}",
        "--dataset=silver_claims",
        f'--views={",".join([config["name"] for config in configs])}',
        f"--date={xcom_pull('date', config_to_xcom.task_id)}",
    ],
)

daily_carefirst_member_attribution = KubernetesStep(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    task_id="daily_carefirst_member_attribution",
    cmds=[
        "bin/AttributionRunner",
        "--project=cityblock-data",
        "--deployTo=carefirstproduction",
        f"--bigQueryProject={PAYER_PROJECT}",
        "--bigQueryDataset=silver_claims",
        "--environment=prod",
        "--tempLocation=gs://internal-tmp-cbh/temp",
    ],
)

load_demographics = scio_load_daily_demographics_insurance_extract
update_views = update_views_for_demographics_and_member_month
run_attribution = daily_carefirst_member_attribution

(config_to_xcom >> load_demographics >> update_views >> run_attribution)
