import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import airflow_utils

# Kubernetes Secret Volume class, ensure `secret` is available in k8s cluster
secret_mount = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-example-scio-job",
    key="key.json",
)


# instantiating DAG object, must be referenced by all tasks
dag = DAG(
    dag_id="example_scio_v1",
    start_date=datetime.datetime(2021, 1, 17),
    schedule_interval="0 0 1/4 * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

first_scio = KubernetesPodOperator(
    dag=dag,
    # bug prevents us from getting logs in real time: https://issues.apache.org/jira/browse/AIRFLOW-4526
    get_logs=False,
    secrets=[secret_mount],
    # this allows pod to access gcloud services
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    task_id="first-scio",
    # Name of task you want to run, used to generate Pod ID.
    name="first-scio",
    # Entrypoint of the container, if not specified the Docker container's
    # entrypoint is used.
    cmds=[
        "bin/start-example",
        "--project=cbh-tanjin-panna",
        "--runner=DataflowRunner",
        "--input=clouddataflow-readonly:samples.weather_stations",
        "--output=start_example.bigquery_tornadoes",
        "--subnetwork=regions/us-central1/subnetworks/example-subnet",
    ],
    # The namespace to run within Kubernetes, default namespace is
    # `default`. There is the potential for the resource starvation of
    # Airflow workers and scheduler within the Cloud Composer environment,
    # the recommended solution is to increase the amount of nodes in order
    # to satisfy the computing requirements. Alternatively, launching pods
    # into a custom namespace will stop fighting over resources.
    namespace="default",
    image="gcr.io/cbh-tanjin-panna/starter:latest",  # source: https://github.com/tanjinP/scio_example
    image_pull_policy="Always",
)

second_scio = KubernetesPodOperator(
    dag=dag,
    get_logs=False,
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    task_id="downstream-scio",
    name="second-scio",
    cmds=[
        "bin/downstream-job",
        "--project=cbh-tanjin-panna",
        "--runner=DataflowRunner",
        "--subnetwork=regions/us-central1/subnetworks/example-subnet",
    ],
    namespace="default",
    image="gcr.io/cbh-tanjin-panna/starter:latest",
    image_pull_policy="Always",
)

first_scio >> second_scio
