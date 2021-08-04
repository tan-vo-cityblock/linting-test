import datetime

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import DAG

import airflow_utils

dag = DAG(
    dag_id="emblem_mor_v1",
    start_date=datetime.datetime(2021, 2, 22),  # Data is usually delivered by the 21st.
    schedule_interval="0 0 22 * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

year_month = (datetime.datetime.now()).strftime("%Y%m")

load_mor_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="load_mor_to_bigquery",
    bucket="cbh_sftp_drop",
    source_objects=["emblem/drop/CITYBLOCK_MOR_" + year_month + ".txt"],
    destination_project_dataset_table=f"emblem-data.cms_revenue.mor_{year_month}01",
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    skip_leading_rows=1,
    dag=dag,
)
