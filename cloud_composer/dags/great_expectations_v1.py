import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import default_args

gcp_secret_path = "/var/secrets/google"
svc_acct_secret = secret.Secret(
    deploy_type="volume",
    deploy_target=gcp_secret_path,
    secret="tf-svc-great-expectations",
    key="key.json",
)

slack_webhook_env = secret.Secret(
    deploy_type="env",
    deploy_target="SLACK_WEBHOOK",
    secret="prod-ge-slack-webhook",
    key="latest",
)

dag = DAG(
    dag_id="ge_dev_validate_v1",
    # 11AM UTC => 6AM EST / 7AM EDT
    start_date=datetime.datetime(2021, 1, 17, 6 + 5),
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args,
    catchup=False,
)

validate_run = KubernetesPodOperator(
    dag=dag,
    image="us.gcr.io/cbh-git/great_expectations:latest",
    image_pull_policy="Always",
    namespace="default",
    name="validate-professional-connecticare",
    task_id="validate_professional_connecticare",
    get_logs=True,
    secrets=[svc_acct_secret, slack_webhook_env],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    cmds=[
        "python",
        "src/validate_expectations.py",
        "--env=prod",
        "--dataset=professional",
        "--partner=connecticare",
    ],
)
