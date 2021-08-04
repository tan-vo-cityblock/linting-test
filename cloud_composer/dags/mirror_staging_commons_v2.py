import datetime

from airflow.models import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

from airflow_utils import GCP_ENV_VAR, default_args

# Various config variables
environment = "staging"
aptible_database_name = "commons-staging"
gcs_export_bucket = "cbh-export-commons-staging"
database_name = "staging"
gcp_project = "cbh-db-mirror-staging"
bq_output_dataset = "commons_mirror"
svc_acct = "staging-commons-mirror@cbh-db-mirror-staging.iam.gserviceaccount.com"
gcs_temp_location = "gs://db-mirror-dataflow-temp-staging/temp"
DRAFTJS_TABLES = [
    "note",
    "note_revision",
    "goal_comment",
    "task_comment",
]  # missing note_draft as Aptible script filters it out


def tables_for_views(tables_copied: str) -> str:
    """This is the place to aggregate all the tables to update a view from as a result of the
    mirror logic or additional transformations

    Sample output:
    "table1,table2,table3,table4_transformed,table5_transformed"

    :param tables_copied: XCom value from the tables ported by Aptible
    :return: single string containing all the tables to update a view for delimited by a comma.
    """
    new_tables = map(
        lambda draftjs_table: f"{draftjs_table}_transformed", DRAFTJS_TABLES
    )
    return tables_copied + "," + ",".join(new_tables)


cbh_export_volume_config = {"emptyDir": {}}
cbh_export_volume = Volume(name="cbh-export", configs=cbh_export_volume_config)
cbh_export_volume_mount = VolumeMount(
    name="cbh-export", mount_path="/cbh-export", sub_path="", read_only=False
)

gcp_secret_path = "/var/secrets/google"
gcp_secrets = secret.Secret(
    deploy_type="volume",
    deploy_target=gcp_secret_path,
    secret="tf-svc-staging-commons-mirror",
    key="key.json",
)

aptible_user_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_USER",
    secret="aptible-staging-secrets",
    key="username",
)
aptible_pass_env = secret.Secret(
    deploy_type="env",
    deploy_target="APTIBLE_PASSWORD",
    secret="aptible-staging-secrets",
    key="password",
)

commons_token_env = secret.Secret(
    deploy_type="env",
    deploy_target="COMMONS_TOKEN",
    secret="commons-github-secrets",
    key="github_token",
)

dag = DAG(
    dag_id="staging_commons_mirroring_v2",
    start_date=datetime.datetime(2021, 1, 19, 0 + 5),  # 12AM EST (1AM EDT)
    schedule_interval="0 5 * * *",
    default_args=default_args,
    catchup=False,
)

aptible_to_gcs_export = KubernetesPodOperator(
    dag=dag,
    name="aptible-to-gcs-export",
    task_id="aptible-to-gcs-export",
    image="gcr.io/cityblock-data/commons-to-bq-mirroring:latest",
    image_pull_policy="Always",
    volumes=[cbh_export_volume],
    volume_mounts=[cbh_export_volume_mount],
    namespace="default",
    secrets=[gcp_secrets, aptible_user_env, aptible_pass_env, commons_token_env],
    env_vars=GCP_ENV_VAR,
    get_logs=False,
    cmds=[
        "/bin/bash",
        "commons_mirror_v2.sh",
        f"-e{environment}",
        f"-p{aptible_database_name}",
        f"-i{gcp_project}",
        f"-b{gcs_export_bucket}",
        "-d{{ next_ds_nodash }}",
    ],
    do_xcom_push=True,
)

scio_mirroring_job = KubernetesPodOperator(
    dag=dag,
    name="scio-mirroring-job",
    task_id="scio-mirroring-job",
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    image_pull_policy="Always",
    namespace="default",
    secrets=[gcp_secrets],
    env_vars=GCP_ENV_VAR,
    get_logs=False,
    cmds=[
        "bin/DatabaseMirroringRunner",
        f"--environment={environment}",
        f"--project={gcp_project}",
        f"--runner=DataflowRunner",
        f"--inputBucket={gcs_export_bucket}",
        f"--outputProject={gcp_project}",
        f"--outputDataset={bq_output_dataset}",
        f"--databaseName={database_name}",
        "--tables={{ task_instance.xcom_pull(task_ids='aptible-to-gcs-export') }}",
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
    secrets=[gcp_secrets],
    env_vars=GCP_ENV_VAR,
    get_logs=False,
    cmds=[
        "python",
        "./update_bq_views.py",
        f"--project={gcp_project}",
        f"--dataset={bq_output_dataset}",
        f"""--tables={tables_for_views("{{ task_instance.xcom_pull(task_ids='aptible-to-gcs-export') }}")}""",
        "--date={{ next_ds_nodash }}",
    ],
)

for table in DRAFTJS_TABLES:
    draftjs_transform = KubernetesPodOperator(
        dag=dag,
        name=f"draftjs-transform-{table}",
        task_id=f"draftjs_transform_{table}",
        image="us.gcr.io/cityblock-data/draftjs-transformer:latest",
        image_pull_policy="Always",
        namespace="default",
        secrets=[gcp_secrets],
        volumes=[cbh_export_volume],
        volume_mounts=[cbh_export_volume_mount],
        env_vars=GCP_ENV_VAR,
        cmds=[
            "npm",
            "run",
            "transform",
            gcp_project,
            bq_output_dataset,  # read and output dataset are the same
            f"{table}_{{{{ next_ds_nodash }}}}",
            gcs_export_bucket,
            bq_output_dataset,
            f"{table}_transformed_{{{{ next_ds_nodash }}}}",
            "/cbh-export",
        ],
    )
    scio_mirroring_job >> draftjs_transform
    draftjs_transform >> update_bq_views

aptible_to_gcs_export >> scio_mirroring_job
scio_mirroring_job >> update_bq_views
