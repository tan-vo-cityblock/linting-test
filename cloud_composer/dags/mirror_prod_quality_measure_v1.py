import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import default_args

# Various config variables
environment = "prod"
gcs_export_bucket = "cbh-cloud-sql-export-quality-measure-prod"
instance_connection_name = "cbh-services-prod:us-east1:services"
instance_project = "cbh-services-prod"
instance_name = "services"
database_name = "quality_measure"
bq_project = "cbh-db-mirror-prod"
bq_output_dataset = "quality_measure_mirror"
svc_acct = "prod-quality-measure-mirror@cbh-db-mirror-prod.iam.gserviceaccount.com"
gcs_temp_location = "gs://db-mirror-dataflow-temp-prod/temp"

# Kubernetes Secret Volume class, ensure `secret` is available in k8s cluster
gcp_secret_path = "/var/secrets/google"
gcp_secrets = secret.Secret(
    deploy_type="volume",
    deploy_target=gcp_secret_path,
    secret="tf-svc-prod-quality-measure-mirror",
    key="key.json",
)

# Postgres creds Volume class
user_env = secret.Secret(
    deploy_type="env",
    deploy_target="USER",
    secret="cbh-db-mirror-quality-measure-prod-secrets",
    key="mirror-user-name",
)
pass_env = secret.Secret(
    deploy_type="env",
    deploy_target="PASS",
    secret="cbh-db-mirror-quality-measure-prod-secrets",
    key="mirror-user-password",
)

# instantiating DAG object, must be referenced by all tasks
# Running at 11AM UTC to follow Elation Mirror dag run at 10AM UTC. Soft dependency so not using ExternalTaskSensor()
dag = DAG(
    dag_id="prod_quality_measure_mirroring_v1",
    start_date=datetime.datetime(2021, 1, 19, 11),  # 11AM UTC => 6AM EST / 7AM EDT
    schedule_interval="0 11 * * *",
    default_args=default_args,
    catchup=False,
)

cloud_sql_config_gathering = KubernetesPodOperator(
    dag=dag,
    name="cloud-sql-config-gathering",
    task_id="cloud-sql-config-gathering",
    image="gcr.io/cityblock-data/cloud-sql-to-bq-mirroring:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets, user_env, pass_env],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{gcp_secret_path}/key.json"},
    get_logs=False,
    cmds=[
        "python",
        "./get_tables_for_cloud_sql_database.py",
        f"--instance={instance_connection_name}",
        f"--database={database_name}",
    ],
    do_xcom_push=True,
)

cloud_sql_gcs_dump = KubernetesPodOperator(
    dag=dag,
    name="cloud-sql-dump-to-gcs",
    task_id="cloud-sql-dump-to-gcs",
    image="gcr.io/cityblock-data/cloud-sql-to-bq-mirroring:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets, user_env, pass_env],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{gcp_secret_path}/key.json"},
    get_logs=False,
    cmds=[
        "python",
        "./cloud_sql_dump_to_gcs.py",
        f"--project={instance_project}",
        f"--instance={instance_name}",
        f"--database={database_name}",
        "--tables={{ task_instance.xcom_pull(task_ids='cloud-sql-config-gathering') }}",
        f"--output-bucket={gcs_export_bucket}",
        f"--key-path={gcp_secret_path}/key.json",
        "--date={{ next_ds_nodash }}",
    ],
)

scio_mirroring_job = KubernetesPodOperator(
    dag=dag,
    name="scio-mirroring-job",
    task_id="scio-mirroring-job",
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{gcp_secret_path}/key.json"},
    get_logs=False,
    cmds=[
        "bin/DatabaseMirroringRunner",
        f"--environment={environment}",
        f"--project={bq_project}",
        f"--runner=DataflowRunner",
        f"--inputBucket={gcs_export_bucket}",
        f"--outputProject={bq_project}",
        f"--outputDataset={bq_output_dataset}",
        f"--databaseName={database_name}",
        "--tables={{ task_instance.xcom_pull(task_ids='cloud-sql-config-gathering') }}",
        "--shardDate={{ next_ds }}",
        "--workerMachineType=n1-standard-2",
        "--numWorkers=1",
        f"--serviceAccountName={svc_acct}",
        f"--tempLocation={gcs_temp_location}",
    ],
)

update_bq_views = KubernetesPodOperator(
    dag=dag,
    name="update-bq-views",
    task_id="update-bq-views",
    image="gcr.io/cityblock-data/cloud-sql-to-bq-mirroring:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets, user_env, pass_env],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": f"{gcp_secret_path}/key.json"},
    get_logs=False,
    cmds=[
        "python",
        "./update_bq_views.py",
        f"--project={bq_project}",
        f"--dataset={bq_output_dataset}",
        "--tables={{ task_instance.xcom_pull(task_ids='cloud-sql-config-gathering') }}",
        "--date={{ next_ds_nodash }}",
    ],
)

cloud_sql_config_gathering >> cloud_sql_gcs_dump
cloud_sql_gcs_dump >> scio_mirroring_job
scio_mirroring_job >> update_bq_views
