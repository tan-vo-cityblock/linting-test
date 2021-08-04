import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret

import airflow_utils

""" Delivers data to Elation on a nightly schedule

Schedule: Every day
Workflow: 
  1) Runs a set of queries on BQ and stores the results as CSVs on GCS
  2) Sync the contents of the files populated from (1) into the Elation S3 bucket (overwrites)

Status: this is expected to run after the commons-mirror job which is still not a part of Airflow, so scheduling is
 'soft' in the sense we expect the dbt run to be well after that job is complete.

Expectation: run every day and submit an updated set of metrics to Elation
This workflow will be daily on a temporary basis until we can figure out a better diff strategy on sending/receiving
 data from Elation

"""

gc_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-elation-worker",
    key="key.json",
)

aws_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/root/.aws",
    secret="elation-aws-secrets",
    key="credentials",
)

elation_image = "us.gcr.io/cityblock-data/elation-to-s3:latest"

dag = DAG(
    dag_id="cbh_to_elation_v1",
    description="dropping off Elation data",
    start_date=datetime.datetime(2021, 6, 18),
    schedule_interval="5 20 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

bq_to_gcs = KubernetesPodOperator(
    image=elation_image,
    namespace="default",
    name="bq-to-gcs",
    task_id="bq_to_gcs",
    secrets=[gc_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "python",
        "./cbh_to_elation.py",
        "--project=cityblock-orchestration",
        f"--gcs-bucket={airflow_utils.GCS_BUCKET}",
        "--date={{ next_ds }}",
    ],
    dag=dag,
)

to_elation_caregaps = KubernetesPodOperator(
    image="google/cloud-sdk:slim",
    namespace="default",
    name="to-elation-caregaps",
    task_id="to_elation_caregaps",
    secrets=[gc_secret, aws_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    arguments=[
        "gsutil",
        "-m",
        "rsync",
        "-r",
        "gs://{bucket}/elation_caregaps_drop/{{{{ next_ds }}}}/".format(
            bucket=airflow_utils.GCS_BUCKET
        ),  # mixing .format w/ jinja requires the jinja template to be escaped (extra {{ }} characters)
        "s3://6f28ec05313a708e-elation-data-import/caregaps/incoming/",
    ],
    dag=dag,
)

to_elation_measures = KubernetesPodOperator(
    image="google/cloud-sdk:slim",
    namespace="default",
    name="to-elation-measures",
    task_id="to_elation_measures",
    secrets=[gc_secret, aws_secret],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    arguments=[
        "gsutil",
        "-m",
        "rsync",
        "-r",
        "gs://{bucket}/elation_measures_drop/{{{{ next_ds }}}}/".format(
            bucket=airflow_utils.GCS_BUCKET
        ),  # mixing .format w/ jinja requires the jinja template to be escaped (extra {{ }} characters)
        "s3://6f28ec05313a708e-elation-data-import/measures/incoming/",
    ],
    dag=dag,
)

bq_to_gcs >> [to_elation_caregaps, to_elation_measures]
