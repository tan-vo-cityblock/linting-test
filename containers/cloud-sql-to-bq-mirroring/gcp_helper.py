from datetime import datetime as dt
from typing import List

from google.cloud.bigquery import (
    Client as BQClient,
    LoadJobConfig,
    StandardSqlDataTypes as BQType,
    SchemaField,
    SchemaUpdateOption,
    SourceFormat,
    Table,
    TimePartitioning,
    TimePartitioningType,
    WriteDisposition,
)
from google.cloud.storage import Client as StorageClient
from pandas import DataFrame

NULL_MARKER = "NULL"


class StorageHelper:
    """Helper class to abstract out commons functions for up/downloading to GCS.

    Implement other GCS helper functions here as needed.
    """

    def __init__(self, project: str):
        self.client = StorageClient(project)
        self.project = project

    def upload_df_as_csv(
        self,
        bucket_name: str,
        gcs_uri: str,
        df: DataFrame,
        null_marker: str = NULL_MARKER,
        include_header: bool = True,
    ) -> str:
        bucket = self.client.bucket(bucket_name)
        gcs_uri_ext = f"{gcs_uri}.csv"

        blob = bucket.blob(gcs_uri_ext)

        # TODO: Investigate limits on df.to_csv() when csv is saved in memory, and look into to_csv compression (https://github.com/pandas-dev/pandas/issues/22555).
        # For now, df is being chunked from reads of Elation db so to_csv memory hit is limited.
        blob.upload_from_string(
            df.to_csv(index=False, na_rep=null_marker, header=include_header),
            content_type="text/csv",
        )

        full_gcs_uri = f"gs://{bucket_name}/{gcs_uri_ext}"
        return full_gcs_uri

    def compose_csv_objects(self, bucket_name: str, gcs_uri_chunk_prefix: str):
        bucket = self.client.bucket(bucket_name)
        all_blob_chunks = list(
            self.client.list_blobs(bucket, prefix=gcs_uri_chunk_prefix)
        )

        gcs_uri_ext = f"{gcs_uri_chunk_prefix}.csv"

        # Check that composite object, which will have name = gcs_uri_ext, doesn't already exist.
        for blob in all_blob_chunks:
            if blob.name == gcs_uri_ext:
                raise ValueError(
                    f'Composite object at "{gcs_uri_ext}"" already exists.'
                )

        composed_blob = bucket.blob(gcs_uri_ext)
        composed_blob.compose(all_blob_chunks)

        full_gcs_uri = f"gs://{bucket_name}/{gcs_uri_ext}"
        return full_gcs_uri


class BQHelper:
    """Helper class to abstract out common functions interacting with BQ datasets/tables.

    Implement other BQ helper functions here as needed.
    """

    BQ_STRING = BQType.STRING.name
    BQ_INTEGER = BQType.INT64.name
    BQ_BOOL = BQType.BOOL.name
    BQ_FLOAT = BQType.FLOAT64.name
    BQ_TIMESTAMP = BQType.TIMESTAMP.name
    BQ_DATE = BQType.DATE.name
    BQ_DATETIME = BQType.DATETIME.name
    BQ_NULLABLE = "NULLABLE"
    BQ_REQUIRED = "REQUIRED"

    def __init__(self, project: str):
        self.client = BQClient(project)
        self.project = project

    def load_from_gcs(
        self,
        gcs_uri: str,
        dataset_name: str,
        table_name: str,
        for_csv: bool = False,
        allow_overwrite: bool = False,
        max_bad_records: int = 0,
        allow_schema_updates: bool = False,
        partition_date: str = None,
        schema: List[SchemaField] = [],
    ):
        if bool(partition_date):
            table_name = f"{table_name}${partition_date}"

        dataset = self.client.get_dataset(f"{self.project}.{dataset_name}")
        table_ref = dataset.table(table_name)

        job_config = self._mk_load_job_config(
            for_csv=for_csv,
            load_partition=bool(partition_date),
            allow_overwrite=allow_overwrite,
            allow_schema_updates=allow_schema_updates,
            max_bad_records=max_bad_records,
            schema=schema,
        )
        job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        try:
            job.result()
            if bool(partition_date):
                table = self.client.get_table(table_ref)
                table.require_partition_filter = True
        except Exception as ex:
            # TODO: See if you can add job.errors: if job.errors else [ex]
            errors = job.errors + [ex] if job.errors else [ex]
            errors_str = "\n".join(str(er) for er in errors)
            raise IOError(errors_str)

    def update_view_for_latest(
        self,
        dataset_name: str,
        table_name: str,
        partition_date: str = None,
        shard_date: str = None,
        allow_overwrite: bool = False,
    ):

        dataset = self.client.get_dataset(f"{self.project}.{dataset_name}")

        if partition_date:
            self._validate_date_format_for_shard_or_partition(partition_date)
            partition_date_for_query = self._format_partition_date_for_query(
                partition_date
            )
            view_id = dataset.table(f"{table_name}_latest")
            view_query = f'SELECT * FROM `{self.project}.{dataset_name}.{table_name}` WHERE _PARTITIONDATE = "{partition_date_for_query}"'
        elif shard_date:
            self._validate_date_format_for_shard_or_partition(shard_date)
            view_id = dataset.table(f"{table_name}")
            view_query = f"SELECT * FROM `{self.project}.{dataset_name}.{table_name}_{shard_date}`"
        else:
            raise ValueError('Must set "partition_date" or "shard_date" parameter')

        view = Table(view_id)
        view.view_query = view_query

        if allow_overwrite:
            self.client.delete_table(view, not_found_ok=True)

        self.client.create_table(view)

    def _mk_load_job_config(
        self,
        for_csv: bool = False,
        load_partition: bool = False,
        allow_overwrite: bool = False,
        allow_schema_updates: bool = False,
        max_bad_records: int = 0,
        schema: List[SchemaField] = [],
    ) -> LoadJobConfig:

        config = LoadJobConfig(max_bad_records=max_bad_records)
        if schema:
            config.schema = schema
        if allow_schema_updates:
            config.schema_update_options = [
                SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        if allow_overwrite:
            config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        if load_partition:
            config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY)
        if for_csv:
            config.allow_quoted_newlines = True
            config.skip_leading_rows = 1
            config.source_format = SourceFormat.CSV
            config.null_marker = NULL_MARKER

        return config

    @staticmethod
    def _validate_date_format_for_shard_or_partition(date_str: str):
        try:
            dt.strptime(date_str, "%Y%m%d")
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYYMMDD")

    def _format_partition_date_for_query(self, partition_date: str) -> str:
        self._validate_date_format_for_shard_or_partition(partition_date)
        date = dt.strptime(partition_date, "%Y%m%d")
        return date.strftime("%Y-%m-%d")
