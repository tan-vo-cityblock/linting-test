import datetime
import re

from airflow.contrib.kubernetes.secret import Secret
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import default_args
from preprocess_payer_data import (
    download_payer_file,
    transform_payer_file,
    delete_local_payer_files,
)
from airflow_utils import GCS_BUCKET

SFTP_SUBDIRS = [
    download_payer_file.EMBLEM_SFTP_SUBDIR,
    download_payer_file.CONNECTICARE_SFTP_SUBDIR,
]
DAG_ID = "load_payer_suspect_data_v1"
NY_PARTNER_NAME = re.sub(r"_.*", "", download_payer_file.EMBLEM_SFTP_SUBDIR)
CT_PARTNER_NAME = re.sub(r"_.*", "", download_payer_file.CONNECTICARE_SFTP_SUBDIR)

secret_mount = Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-process-payer-suspect",
    key="key.json",
)

dag = DAG(
    dag_id=DAG_ID,
    description="daily job to pre-process and load suspect payer data to BigQuery",
    start_date=datetime.datetime(2021, 1, 19, 3 + 5),  # running at 3am EST
    schedule_interval="0 8 * * *",
    default_args=default_args,
    catchup=False,
)

download_payer_file_locally = PythonOperator(
    task_id="download_payer_suspect_data_from_gcs",
    python_callable=download_payer_file.main,
    op_kwargs={
        "gcs_bucket_prefixes": [
            "gs://cbh_sftp_drop/emblem/drop/EH_DxSuspect_",
            "gs://cbh_sftp_drop/connecticare_production/drop/Cityblock_dt",
            # 'gs://cbh_sftp_drop/connecticare_production/drop/CityBlock_dt'
            # TODO: the above files are not included in this workflow, a schema has not been established for them
        ]
    },
    do_xcom_push=True,
    provide_context=True,
    dag=dag,
)


def files_ready_for_processing(**context):
    if context["ti"].xcom_pull(task_ids="download_payer_suspect_data_from_gcs"):
        return "transform_payer_file_data_locally"
    else:
        return "no_files_found"


check_files_found = BranchPythonOperator(
    task_id="are_files_ready_for_processing",
    python_callable=files_ready_for_processing,
    provide_context=True,
    dag=dag,
)

no_files_to_download = DummyOperator(task_id="no_files_found", dag=dag)

transform_payer_files = PythonOperator(
    task_id="transform_payer_file_data_locally",
    python_callable=transform_payer_file.main,
    provide_context=True,
    dag=dag,
)

move_temp_files = [
    GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id=f"move_files_to_prod_bucket_{payer_source}",
        source_bucket=GCS_BUCKET,
        source_object=f"data/payer_suspect_coding_data/{payer_source}/*PROCESSED.csv",
        destination_bucket="cbh_sftp_drop",
        destination_object=f"{payer_source}/drop/",
        move_object=True,
        google_cloud_storage_conn_id="process_payer_suspect",
        dag=dag,
    )
    for payer_source in SFTP_SUBDIRS
]

# TODO: When we upgrade Airflow we can substitute the below with GoogleCloudStorageDeleteOperator
delete_temp_files = PythonOperator(
    task_id="delete_temp_files_in_airflow",
    python_callable=delete_local_payer_files.main,
    op_kwargs={
        "gcs_bucket_prefixes": [
            f"gs://{GCS_BUCKET}/data/payer_suspect_coding_data/{download_payer_file.EMBLEM_SFTP_SUBDIR}",
            f"gs://{GCS_BUCKET}/data/payer_suspect_coding_data/{download_payer_file.CONNECTICARE_SFTP_SUBDIR}",
        ]
    },
    dag=dag,
)


# TODO: The job below is not resilient for the chance that we receive > 1 file/month on 1 day from 1 partner
def check_processed_file_availability(**context):
    ready_tasks = []
    if context["ti"].xcom_pull(
        task_ids="transform_payer_file_data_locally",
        key=download_payer_file.EMBLEM_SFTP_SUBDIR,
    ):
        ready_tasks.append("load_payer_files_to_bq_{}".format(NY_PARTNER_NAME))
    if context["ti"].xcom_pull(
        task_ids="transform_payer_file_data_locally",
        key=download_payer_file.EMBLEM_SFTP_SUBDIR + "_med",
    ):
        ready_tasks.append("load_payer_files_to_bq_med_{}".format(NY_PARTNER_NAME))
    if context["ti"].xcom_pull(
        task_ids="transform_payer_file_data_locally",
        key=download_payer_file.CONNECTICARE_SFTP_SUBDIR,
    ):
        ready_tasks.append("load_payer_files_to_bq_{}".format(CT_PARTNER_NAME))
    if context["ti"].xcom_pull(
        task_ids="transform_payer_file_data_locally",
        key=download_payer_file.CONNECTICARE_SFTP_SUBDIR + "_med",
    ):
        ready_tasks.append("load_payer_files_to_bq_med_{}".format(CT_PARTNER_NAME))
    return ready_tasks


check_processed_files = BranchPythonOperator(
    task_id="are_files_ready_for_load_to_silver",
    python_callable=check_processed_file_availability,
    provide_context=True,
    dag=dag,
)

load_to_silver_commercial_ny = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-payer-files",
    task_id=f"load_payer_files_to_bq_{NY_PARTNER_NAME}",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        f"--outputProject={NY_PARTNER_NAME}-data",
        f'--deliveryDate={{{{ task_instance.xcom_pull(task_ids="transform_payer_file_data_locally", key={download_payer_file.EMBLEM_SFTP_SUBDIR}) }}}}',
        "--workerMachineType=n1-highcpu-32",
        "--inputConfigBucket=cbh-partner-configs",
        f"--inputConfigPaths={NY_PARTNER_NAME}_silver/SuspectCoding.txt",
        "--numWorkers=2",
    ],
    dag=dag,
)

load_to_silver_commercial_ct = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-payer-files",
    task_id=f"load_payer_files_to_bq_{CT_PARTNER_NAME}",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        f"--outputProject={CT_PARTNER_NAME}-data",
        f'--deliveryDate={{{{ task_instance.xcom_pull(task_ids="transform_payer_file_data_locally", key={download_payer_file.CONNECTICARE_SFTP_SUBDIR}) }}}}',
        "--workerMachineType=n1-highcpu-32",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=cci_silver/SuspectCoding.txt",
        "--numWorkers=2",
    ],
    dag=dag,
)

load_to_silver_medicare_ny = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-payer-files",
    task_id=f"load_payer_files_to_bq_med_{NY_PARTNER_NAME}",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        f"--outputProject={NY_PARTNER_NAME}-data",
        f'--deliveryDate={{{{ task_instance.xcom_pull(task_ids="transform_payer_file_data_locally", key="{download_payer_file.EMBLEM_SFTP_SUBDIR}_med") }}}}',
        "--workerMachineType=n1-highcpu-32",
        "--inputConfigBucket=cbh-partner-configs",
        f"--inputConfigPaths={NY_PARTNER_NAME}_silver/SuspectCoding_med.txt",
        "--numWorkers=2",
    ],
    dag=dag,
)

load_to_silver_medicare_ct = KubernetesPodOperator(
    image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
    namespace="default",
    name="scio-load-payer-files",
    task_id=f"load_payer_files_to_bq_med_{CT_PARTNER_NAME}",
    secrets=[secret_mount],
    env_vars={"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"},
    image_pull_policy="Always",
    cmds=[
        "bin/LoadToSilverRunner",
        "--environment=prod",
        "--project=cityblock-data",
        "--runner=DataflowRunner",
        f"--outputProject={CT_PARTNER_NAME}-data",
        f'--deliveryDate={{{{ task_instance.xcom_pull(task_ids="transform_payer_file_data_locally", key="{download_payer_file.CONNECTICARE_SFTP_SUBDIR}_med") }}}}',
        "--workerMachineType=n1-highcpu-32",
        "--inputConfigBucket=cbh-partner-configs",
        "--inputConfigPaths=cci_silver/SuspectCoding_med.txt",
        "--numWorkers=2",
    ],
    dag=dag,
)

move_processed_files_ny = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id=f"move_files_to_sub_folder_{download_payer_file.EMBLEM_SFTP_SUBDIR}",
    source_bucket="cbh_sftp_drop",
    source_object=f"{download_payer_file.EMBLEM_SFTP_SUBDIR}/drop/*_PROCESSED.csv",
    destination_bucket="cbh_sftp_drop",
    destination_object=f"{download_payer_file.EMBLEM_SFTP_SUBDIR}/drop/pre_processed/",
    move_object=True,
    google_cloud_storage_conn_id="process_payer_suspect",
    dag=dag,
)

move_processed_files_ct = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id=f"move_files_to_sub_folder_{download_payer_file.CONNECTICARE_SFTP_SUBDIR}",
    source_bucket="cbh_sftp_drop",
    source_object=f"{download_payer_file.CONNECTICARE_SFTP_SUBDIR}/drop/*_PROCESSED.csv",
    destination_bucket="cbh_sftp_drop",
    destination_object=f"{download_payer_file.CONNECTICARE_SFTP_SUBDIR}/drop/pre_processed/",
    move_object=True,
    google_cloud_storage_conn_id="process_payer_suspect",
    dag=dag,
)


download_payer_file_locally >> check_files_found
check_files_found >> no_files_to_download

check_files_found >> transform_payer_files >> move_temp_files
move_temp_files >> delete_temp_files >> check_processed_files

check_processed_files >> load_to_silver_commercial_ny >> move_processed_files_ny
check_processed_files >> load_to_silver_medicare_ny >> move_processed_files_ny
check_processed_files >> load_to_silver_commercial_ct >> move_processed_files_ct
check_processed_files >> load_to_silver_medicare_ct >> move_processed_files_ct
