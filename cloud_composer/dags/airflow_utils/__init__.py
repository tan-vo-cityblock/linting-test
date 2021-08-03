from . import cityblock


from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import secret


default_args = {
    "owner": "cityblock",
    "depends_on_past": False,
    "email": [Variable.get("slack_email")],
    "email_on_retry": False,
    "retries": 0,
    # KubernetesPodOperator: overriding default - we want a fresh pod on restarts
    "reattach_on_restart": False,
}

# emails of relevant data stakeholders; used for emails
data_group = [
    "data-team@cityblock.com",
    "gcp-admins@cityblock.com",
    "actuary@cityblock.com",
    "partner-data-eng@cityblock.com",
]
cf_email = ["katie.claiborne@cityblock.com"]

# used to point to production Airflow UI; used for emails
prod_link = (
    "https://c0c6ffe42d9557c2cp-tp.appspot.com/admin/airflow/tree?dag_id={dag_id}"
)

# Storage bucket
GCS_BUCKET = Variable.get("bucket")

GCP_ENV_VAR = {"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"}
DBT_ENV_VARS = {**GCP_ENV_VAR, "DBT_PROFILES_DIR": "/profiles"}

"""HELPER FUNCTIONS"""


def bq_result_html(description: str, dag_id: str) -> str:
    """Returns a simple HTML render of a message for a job that was complete along w/ link to the DAG itself

    :param description: friendly name of the DAG
    :param dag_id:      DAG ID used for URL
    """
    return """
    <html>
    {description} job is complete, the relevant tables should now be visible on BigQuery.
    <br>
    <a href="{url}">View the DAG here</a>
    <br>
    This is an automated email, please do not reply to this.
    </html>
    """.format(
        description=description, url=prod_link.format(dag_id=dag_id)
    )


"""TASK TEMPLATES"""
PROD_DBT_CREDS = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-dbt-prod",
    key="key.json",
)


def dbt_k8s_pod_op(
    dbt_cmd: str, task_name: str, dag, trigger_rule="all_success"
) -> KubernetesPodOperator:
    """Generic KubernetesPodOperator meant for running all dbt tasks on Airflow

    :param dbt_cmd:         dbt commands to execute (put in bash format)
    :param task_name:       name of task (must be snake_case)
    :param dag:             DAG associated with the task
    :param trigger_rule:    Trigger rule for the task, defaulted to Airflow default (success required, otherwise fail DAG)
    :return:                KubernetesPodOperator that can be used in any given DAG
    """
    return KubernetesPodOperator(
        dag=dag,
        get_logs=True,
        secrets=[PROD_DBT_CREDS],
        env_vars=DBT_ENV_VARS,
        task_id=task_name,
        name=task_name.replace("_", "-"),
        cmds=["/bin/bash", "-c"],
        arguments=[dbt_cmd],
        namespace="default",
        image="gcr.io/cityblock-orchestration/dbt:latest",
        image_pull_policy="Always",
        trigger_rule=trigger_rule,
    )
