import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret

import airflow_utils

""" Delivers data to Able Health on a nightly schedule

Schedule: Every day
Workflow: 
  1) Runs a set of queries on BQ and stores the results as CSVs on GCS
  2) Sync the contents of the files populated from (1) into the Able Health S3 bucket (overwrites)

Status: this is expected to run after the commons-mirror job which is still not a part of Airflow, so scheduling is
 'soft' in the sense we expect the dbt run to be well after that job is complete.

Expectation: run every day and submit an updated set of metrics to Able Health
This workflow will be daily on a temporary basis until we can figure out a better diff strategy on sending/receiving
 data from Able Health

"""

gc_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-able-health",
    key="key.json",
)

aws_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/root/.aws",
    secret="ablehealth-prod-secrets",
    key="credentials",
)

ablehealth_image = "us.gcr.io/cityblock-data/ablehealth:latest"

dag = DAG(
    dag_id="cbh_to_able_v1",
    description="dropping off able data",
    start_date=datetime.datetime(
        2021, 1, 19
    ),  # every night at 7PM will be the delivery time
    schedule_interval="0 0 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

bq_to_gcs = KubernetesPodOperator(
    image=ablehealth_image,
    namespace="default",
    name="bq-to-gcs",
    task_id="bq_to_gcs",
    secrets=[gc_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./cbh_to_able.py",
        "--project=cityblock-orchestration",
        f"--gcs-bucket={airflow_utils.GCS_BUCKET}",
        "--date={{ next_ds }}",
    ],
    dag=dag,
)

to_able = KubernetesPodOperator(
    image="google/cloud-sdk:slim",
    namespace="default",
    name="to-able",
    task_id="to_able",
    secrets=[gc_secret, aws_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    arguments=[
        "gsutil",
        "-m",
        "rsync",
        "-r",
        "gs://{bucket}/able_health_drop/{{{{ next_ds }}}}/".format(
            bucket=airflow_utils.GCS_BUCKET
        ),  # mixing .format w/ jinja requires the jinja template to be escaped (extra {{ }} characters)
        "s3://ablehealth-data-transfer/cityblock/import/",
    ],
    dag=dag,
)

bq_to_gcs >> to_able
