import datetime

from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

import airflow_utils

""" Backup SFTP data to cold storage
Schedule: Every day
Workflow:
  1) takes all contents of SFTP bucket and copies it to cold storage name spaced by time of copy

Expectation: run at defined cadence and backup all the data specified
"""

svc_acct = Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-sftp-backup",
    key="key.json",
)

dag = DAG(
    dag_id="sftp_backup_v1",
    description="Backup SFTP data",
    start_date=datetime.datetime(2021, 1, 19, 17 + 5),  # ingests at 5PM EST
    schedule_interval="0 22 * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

SFTP_SOURCE_BUCKET = "cbh_sftp_drop"
SFTP_BACKUP_BUCKET = "cbh-sftp-drop-backup"


backup_sftp = KubernetesPodOperator(
    # pinning image to release from May 18th 2020
    image="gcr.io/cloud-builders/gcloud-slim@sha256:19665afd1e3deb791abd39dba1737c46322b3e3ff28f591d831f377fb90b7af6",
    namespace="default",
    name="backup-sftp",
    task_id="backup-sftp",
    secrets=[svc_acct],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
        "TIME": '{{ next_execution_date.in_timezone("US/Eastern").strftime("%Y%m%d%H%M%S%Z") }}',
    },
    image_pull_policy="Always",
    cmds=["/bin/bash", "-c"],
    arguments=[
        (
            "gcloud auth activate-service-account --key-file=/var/secrets/google/key.json"
            " && echo saving backup at time: ${TIME}"
            f" && gsutil -m rsync -r gs://{SFTP_SOURCE_BUCKET} gs://{SFTP_BACKUP_BUCKET}/automated_backups"
        )
    ],
    dag=dag,
)
