import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import default_args

# Various config variables
project = "cityblock-data"
runner = "DataflowRunner"
source_project = "cityblock-data"
source_dataset = "src_health_home"
source_table = "health_home_eligibility"
destination_bucket = "gs://cityblock-production-patient-data"
svc_acct = "patient-eligibilities@cityblock-orchestration.iam.gserviceaccount.com"
gcs_temp_location = "gs://internal-tmp-cbh/temp"

# Kubernetes Secret Volume class, ensure `secret` is available in k8s cluster
gcp_secret_path = "/var/secrets/google"
gcp_secrets = secret.Secret(
    deploy_type="volume",
    deploy_target=gcp_secret_path,
    secret="tf-svc-patient-eligibilities",
    key="key.json",
)

# instantiating DAG object, must be referenced by all tasks
dag = DAG(
    dag_id="patient_eligibilities_update_v1",
    start_date=datetime.datetime(2021, 1, 19, 4 + 5),  # 4AM EST
    schedule_interval="0 9 1/3 * *",
    default_args=default_args,
    catchup=False,
)

scio_push_patient_eligibilities = KubernetesPodOperator(
    dag=dag,
    name="push-patient-eligibilities",
    task_id="push-patient-eligibilities",
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{gcp_secret_path}/key.json"},
    get_logs=False,
    cmds=[
        "bin/PushPatientEligibilities",
        f"--project={project}",
        f"--runner={runner}",
        f"--sourceProject={source_project}",
        f"--sourceDataset={source_dataset}",
        f"--sourceTable={source_table}",
        f"--destinationBucket={destination_bucket}",
        "--workerMachineType=n1-standard-2",
        "--numWorkers=1",
        f"--serviceAccountName={svc_acct}",
        f"--tempLocation={gcs_temp_location}",
    ],
)
