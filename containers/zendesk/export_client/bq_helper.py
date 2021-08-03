import json
import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Union

from google.cloud.bigquery import (
    Client,
    LoadJobConfig,
    SchemaField,
    SchemaUpdateOption,
    SourceFormat,
    StandardSqlDataTypes,
    Table,
    TimePartitioning,
    TimePartitioningType,
    WriteDisposition,
)
from google.cloud.exceptions import NotFound
from util import (
    is_list_of_dicts_with_key,
    is_list_of_primitives,
    json_serialize,
    only_one_arg_set,
    stringify_int_val_for_key,
)

logger = logging.getLogger(__name__)


class BQHelper:
    # TODO Make DRY with code in 'mixer/containers/cloud-sql-to-bq-mirroring/gcp_helper.py'
    """Helper class to abstract out common functions interacting with BQ datasets/tables."""

    BQ_STRING = StandardSqlDataTypes.STRING.name
    BQ_INTEGER = StandardSqlDataTypes.INT64.name
    BQ_TIMESTAMP = StandardSqlDataTypes.TIMESTAMP.name
    BQ_DATE = StandardSqlDataTypes.DATE.name
    BQ_REQUIRED = "REQUIRED"

    def __init__(self, project: str):
        self.client = Client(project)
        self.project = project

    def load_json_from_gcs(
        self,
        gcs_uri: str,
        dataset_name: str,
        table_name: str,
        allow_overwrite: bool = False,
        max_bad_records: int = 0,
        allow_schema_updates: bool = False,
        bq_partition: Optional[date] = None,
        schema: Optional[List[SchemaField]] = None,
    ):
        if bool(bq_partition):
            table_name = f"{table_name}${self._mk_date_for_table_ref(bq_partition)}"

        dataset = self.client.get_dataset(f"{self.project}.{dataset_name}")
        table_ref = dataset.table(table_name)

        try:
            table = self.client.get_table(table_ref)
            # if table exists, don't autodetect as there may be conflict between
            # what bq sets as type of all null col in json vs type already set in bq.
            autodetect_schema = False
        except NotFound:
            autodetect_schema = not bool(schema)
            logger.warning(
                (
                    f"Table {table_ref.table_id} does not exist,"
                    f" loading json to new table with autodetect_schema = {autodetect_schema}"
                )
            )

        job_config = self._mk_json_load_job_config(
            load_partition=bool(bq_partition),
            allow_overwrite=allow_overwrite,
            allow_schema_updates=allow_schema_updates,
            max_bad_records=max_bad_records,
            autodetect_schema=autodetect_schema,
            schema=schema,
        )

        job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        try:
            job.result()
            if bool(bq_partition):
                table = self.client.get_table(table_ref)
                table.require_partition_filter = True

            logger.info(
                f"Loaded json from '{gcs_uri}' to table '{table_ref.dataset_id}.{table_ref.table_id}'"
            )
        except Exception as ex:
            errors = job.errors + [ex] if job.errors else [ex]
            errors_str = "\n".join(str(er) for er in errors)
            raise IOError(errors_str)

    def _mk_json_load_job_config(
        self,
        load_partition: bool = False,
        allow_overwrite: bool = False,
        allow_schema_updates: bool = False,
        max_bad_records: int = 0,
        autodetect_schema: bool = False,
        schema: Optional[List[SchemaField]] = None,
    ) -> LoadJobConfig:

        config = LoadJobConfig(
            max_bad_records=max_bad_records,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        if schema:
            config.schema = schema
        else:
            config.autodetect = autodetect_schema

        if allow_schema_updates:
            config.schema_update_options = [
                SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        if allow_overwrite:
            config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        if load_partition:
            config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY)

        return config

    def update_view_for_latest(
        self,
        dataset_name: str,
        table_name: str,
        bq_partition: Optional[date] = None,
        shard_date: Optional[date] = None,
        custom_view_query: Optional[str] = None,
    ):
        if not only_one_arg_set(bq_partition, shard_date, custom_view_query):
            raise ValueError(
                'Must set only one of "partition_date" or "shard_date" or "custom_view_query" parameter'
            )

        dataset = self.client.get_dataset(f"{self.project}.{dataset_name}")

        if bq_partition:
            partition_date_for_query = self._mk_date_for_query(bq_partition)
            view_id = dataset.table(f"{table_name}_latest")
            view_query = (
                f"SELECT * FROM `{self.project}.{dataset_name}.{table_name}` "
                f'WHERE _PARTITIONDATE = "{partition_date_for_query}"'
            )
        elif shard_date:
            shard_date_for_ref = self._mk_date_for_table_ref(shard_date)
            view_id = dataset.table(f"{table_name}")
            view_query = (
                f"SELECT * FROM "
                f"`{self.project}.{dataset_name}.{table_name}_{shard_date_for_ref}`"
            )
        elif custom_view_query:
            view_id = dataset.table(f"{table_name}_latest")
            view_query = custom_view_query

        view = Table(view_id)
        view.view_query = view_query

        self.client.delete_table(view, not_found_ok=True)
        self.client.create_table(view)

    def get_or_create_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        schema: List[SchemaField],
    ) -> Table:
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        try:
            return self.client.get_table(full_table_id)
        except NotFound:
            logger.warning(f"Table {full_table_id} does not exist, creating table now.")
            table = Table(full_table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {full_table_id}")
            return table

    @staticmethod
    def _mk_date_for_table_ref(dt: date) -> str:
        return dt.strftime("%Y%m%d")

    @staticmethod
    def _mk_date_for_query(dt: date) -> str:
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def update_zendesk_json_for_bq(
        json_str: str,
        inserted_at: Optional[datetime] = None,
        inserted_at_col: Optional[str] = None,
    ) -> str:

        cleaned_dict = json.loads(json_str, object_hook=BQHelper._clean_zendesk_json)

        if inserted_at:
            cleaned_dict[inserted_at_col] = inserted_at

        return json.dumps(cleaned_dict, default=json_serialize)

    @staticmethod
    def _clean_zendesk_json(obj):
        if not isinstance(obj, dict):
            return obj
        new_obj = {}
        for key, val in obj.items():
            new_key = key
            new_val = val
            if key.isnumeric():
                # columns that are numeric must start with an underscore
                # see https://cloud.google.com/bigquery/docs/schemas#column_names
                new_key = f"_{key}"
            elif key == "value" and (
                isinstance(val, bool) or is_list_of_primitives(val)
            ):
                # stringify bools and primitive types so BQ can infer one type (str) from mixed types.
                new_val = str(val)
            elif key == "columns" and is_list_of_dicts_with_key(val, "id"):
                new_val = [stringify_int_val_for_key(d, "id") for d in val]
            elif key == "group" and isinstance(val, dict) and "id" in val:
                new_val = stringify_int_val_for_key(val, "id")

            if isinstance(val, dict) and len(val) == 0:
                # bq can't detect nested types in an empty dict/record
                # so set empty dicts to None, will be null in BQ
                new_val = None

            new_obj[new_key] = new_val
        return new_obj


class ZendeskExportMetadata:
    """Class holding metadata on export of zendesk models that will write to BQ.
    Each class model export will be written as a row to `project.dataset.EXPORT_METADATA`
    """

    METADATA_TABLE_NAME = "EXPORT_METADATA"

    # column name values in BQ table zendesk.EXPORT_METADATA
    # must match property names in __init__ method and column names in BQ METDATA_SCHEMA array.
    TABLE_NAME_COL = "table_name"
    BQ_INSERTED_AT_COL = "bq_inserted_at"
    START_TIME_COL = "start_time"
    START_CURSOR_COL = "start_cursor"
    END_TIME_COL = "end_time"
    END_CURSOR_COL = "end_cursor"
    EXPORT_COUNT_COL = "export_count"
    RUN_LENGTH_SEC_COL = "run_length_sec"
    BQ_PARTITION_COL = "bq_partition"

    METADATA_SCHEMA = [
        SchemaField(name=TABLE_NAME_COL, field_type=BQHelper.BQ_STRING),
        SchemaField(name=BQ_INSERTED_AT_COL, field_type=BQHelper.BQ_TIMESTAMP),
        SchemaField(name=START_TIME_COL, field_type=BQHelper.BQ_TIMESTAMP),
        SchemaField(name=START_CURSOR_COL, field_type=BQHelper.BQ_STRING),
        SchemaField(name=END_TIME_COL, field_type=BQHelper.BQ_TIMESTAMP),
        SchemaField(name=END_CURSOR_COL, field_type=BQHelper.BQ_STRING),
        SchemaField(name=EXPORT_COUNT_COL, field_type=BQHelper.BQ_INTEGER),
        SchemaField(name=RUN_LENGTH_SEC_COL, field_type=BQHelper.BQ_INTEGER),
        SchemaField(name=BQ_PARTITION_COL, field_type=BQHelper.BQ_DATE),
    ]

    def __init__(
        self,
        table_name: str,
        bq_inserted_at: datetime,
        export_count: int,
        run_length_sec: int,
        start_time: datetime = None,
        start_cursor: str = None,
        end_cursor: str = None,
        end_time: datetime = None,
        bq_partition: str = None,
    ):
        self.table_name = table_name
        self.bq_inserted_at = bq_inserted_at
        self.start_time = start_time
        self.start_cursor = start_cursor
        self.end_time = end_time
        self.end_cursor = end_cursor
        self.export_count = export_count
        self.run_length_sec = run_length_sec
        self.bq_partition = bq_partition

    def to_json_dict(self) -> Dict[str, Union[str, int]]:
        """BQ method client.insert_rows_json expects rows as python dicts with json serializable
        key/values.
        """
        class_dict = vars(self)
        serialized_dict = json.loads(json.dumps(class_dict, default=json_serialize))
        return serialized_dict
