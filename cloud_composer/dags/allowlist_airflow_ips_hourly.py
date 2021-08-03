import datetime
from airflow.contrib.kubernetes import secret
from airflow.models.dag import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import airflow_utils
from textwrap import dedent


GCP_SECRET_PATH = "/var/secrets/google"
CLOUD_FXN_ENDPOINT = (
    "https://us-east4-cityblock-data.cloudfunctions.net/airflow_ae_firewall_allow"
)
SERVICES = ["cbh-services-prod", "cbh-member-service-prod"]

dag = DAG(
    dag_id="allowlist_airflow_ips_hourly",
    description=dedent(
        """\
        stop-gap solution to app engine (DEPRECIATED) services 
        using ip allowlists as 'security' when airflow nodes 
        randomly change ip address
        """
    ),
    start_date=datetime.datetime(2021, 5, 26),
    schedule_interval="0 * * * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

cf_svc_acct = secret.Secret(
    deploy_type="volume",
    deploy_target=GCP_SECRET_PATH,
    secret="tf-svc-airflow-ae-cf-invoker",
    key="key.json",
)


def allow_airflow_ips_for_service(ae_instance_name: str):
    task_id = f"allow_airflow_ips_for_service_{ae_instance_name}"
    return KubernetesPodOperator(
        # pinning image to release from June 4th 2020
        image="gcr.io/cloud-builders/gcloud-slim@sha256:317353ab0694b7b25d2c71c7b4639b7b31b8b2e8c055ca3e84a25029372a4b9d",
        namespace="default",
        name=task_id.replace("_", "-"),
        task_id=task_id,
        secrets=[cf_svc_acct],
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{GCP_SECRET_PATH}/key.json"},
        image_pull_policy="Always",
        cmds=["/bin/bash", "-c"],
        arguments=[
            (
                f"gcloud auth activate-service-account --key-file={GCP_SECRET_PATH}/key.json"
                f" && curl -X POST {CLOUD_FXN_ENDPOINT}"
                ' -H "Authorization: Bearer $(gcloud auth print-identity-token)"'
                ' -H "Content-Type:application/json"'
                f' -d \'{{"ae_instance": "{ae_instance_name}"}}\''
            )
        ],
        dag=dag,
    )


for ae_instance_name in SERVICES:
    allow_airflow_ips_for_service(ae_instance_name)
