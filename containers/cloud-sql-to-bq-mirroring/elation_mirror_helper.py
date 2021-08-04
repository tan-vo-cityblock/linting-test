import json
import logging
from datetime import datetime as dt
from typing import List, Tuple

import pandas as pd
from google.cloud.bigquery import SchemaField
from pytz import utc

from gcp_helper import BQHelper

logger = logging.getLogger(__name__)


class ElationSecret:
    """Class for loading and easily accessing the numerous Elation secrets
    required to ssh into their server and connect to our instance of the database.

    Initialization is expected to occur via the from_json_file @classmethod
    """

    def __init__(
        self,
        ssh_hostname: str,
        ssh_user: str,
        ssh_private_key_path: str,
        mysql_remote_host: str,
        mysql_remote_port: str,
        mysql_local_host: str,
        mysql_user: str,
        mysql_password: str,
        mysql_database: str,
    ):
        self.ssh_hostname = ssh_hostname
        self.ssh_user = ssh_user
        self.ssh_private_key_path = ssh_private_key_path
        self.mysql_remote_host = mysql_remote_host
        self.mysql_remote_port = mysql_remote_port
        self.mysql_local_host = mysql_local_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database

    @classmethod
    def from_json_file(cls, file_path: str):
        with open(file_path, "r") as f:
            elation_miror_secret_dict = json.load(f)
        return cls(**elation_miror_secret_dict)


class ElationMetadata:
    """Static style Class to hold and group static variables/methods related
    to generating metadata on the tables mirrored from Elation. Will write to `project.dataset.MIRROR_METADATA`

    TODO: Consider abstracting into a super class that other mirror jobs can use.
    """

    TABLE_NAME = "TABLE_NAME"
    TABLE_ROWS = "TABLE_ROWS"
    TABLE_SIZE_MB = "TABLE_SIZE_MB"
    CREATE_TIME = "CREATE_TIME"
    UPDATE_TIME = "UPDATE_TIME"
    DB_TIMEZONE = "DB_TIMEZONE"
    BQ_LOAD_TIME = "BQ_LOAD_TIME"
    BQ_PARTITION = "BQ_PARTITION"

    METADATA_TABLE_NAME = "MIRROR_METADATA"

    MYSQL_NULLABLE_TO_BQ_MODE = {
        "YES": BQHelper.BQ_NULLABLE,
        "NO": BQHelper.BQ_REQUIRED,
    }

    MYSQL_TYPE_TO_BQ_TYPE = {
        "bigint": BQHelper.BQ_INTEGER,
        "int": BQHelper.BQ_INTEGER,
        "smallint": BQHelper.BQ_INTEGER,
        "tinyint": BQHelper.BQ_INTEGER,
        "double": BQHelper.BQ_FLOAT,
        "decimal": BQHelper.BQ_FLOAT,
        "bool": BQHelper.BQ_BOOL,
        "varchar": BQHelper.BQ_STRING,
        "longtext": BQHelper.BQ_STRING,
        "binary": BQHelper.BQ_STRING,
        "date": BQHelper.BQ_DATE,
        "datetime": BQHelper.BQ_DATETIME,
    }

    def __init__(self):
        pass

    @staticmethod
    def get_metadata_bq_schema() -> List[SchemaField]:
        return [
            SchemaField(
                ElationMetadata.TABLE_NAME,
                BQHelper.BQ_STRING,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(
                ElationMetadata.CREATE_TIME,
                BQHelper.BQ_DATETIME,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(ElationMetadata.UPDATE_TIME, BQHelper.BQ_DATETIME),
            SchemaField(
                ElationMetadata.DB_TIMEZONE,
                BQHelper.BQ_STRING,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(
                ElationMetadata.TABLE_SIZE_MB,
                BQHelper.BQ_INTEGER,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(
                ElationMetadata.TABLE_ROWS,
                BQHelper.BQ_INTEGER,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(
                ElationMetadata.BQ_LOAD_TIME,
                BQHelper.BQ_TIMESTAMP,
                mode=BQHelper.BQ_REQUIRED,
            ),
            SchemaField(
                ElationMetadata.BQ_PARTITION,
                BQHelper.BQ_STRING,
                mode=BQHelper.BQ_REQUIRED,
            ),
        ]

    @staticmethod
    def convert_datetime_to_utc_partition_str(row: pd.Series) -> str:
        return (
            row.CREATE_TIME.tz_localize(row.DB_TIMEZONE)
            .tz_convert(utc)
            .strftime("%Y%m%d")
        )

    @staticmethod
    def _get_table_row_count(sqlalchemy_engine, table_name: str) -> str:
        count_col = "count"
        query = f"SELECT COUNT(*) AS {count_col} FROM cityblock.{table_name}"
        count = pd.read_sql_query(query, con=sqlalchemy_engine).at[0, count_col]

        return count

    @staticmethod
    def get_metadata_df(sqlalchemy_engine, load_time: dt) -> pd.DataFrame:
        query = (
            f"SELECT {ElationMetadata.TABLE_NAME}, "
            f"{ElationMetadata.CREATE_TIME}, {ElationMetadata.UPDATE_TIME}, "
            f"(SELECT @@global.time_zone) AS {ElationMetadata.DB_TIMEZONE}, "
            f"CAST(ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS UNSIGNED) AS {ElationMetadata.TABLE_SIZE_MB} "
            "FROM information_schema.TABLES "
            'WHERE TABLE_TYPE = "BASE TABLE" AND TABLE_SCHEMA = "cityblock"'
        )
        metadata_df = pd.read_sql_query(query, sqlalchemy_engine)

        # setting TABLE_ROWS to None initially and will update row count by looping through each table.
        # Not pulling count from information_schema.TABLE_ROWS since it is an approximation: https://stackoverflow.com/a/15796710
        metadata_df[ElationMetadata.TABLE_ROWS] = None
        metadata_df[ElationMetadata.BQ_LOAD_TIME] = load_time
        metadata_df[ElationMetadata.BQ_PARTITION] = metadata_df.apply(
            ElationMetadata.convert_datetime_to_utc_partition_str, axis=1
        )

        for row in metadata_df.itertuples():
            table_name = row.TABLE_NAME
            table_rows_count = ElationMetadata._get_table_row_count(
                sqlalchemy_engine, table_name
            )

            metadata_df.at[row.Index, ElationMetadata.TABLE_ROWS] = table_rows_count
        return metadata_df

    @staticmethod
    def get_table_schema_info(
        sqlalchemy_engine, table_name: str
    ) -> Tuple[List[str], List[SchemaField]]:
        col_name_col = "COLUMN_NAME"
        is_nullable_col = "IS_NULLABLE"
        data_type_col = "DATA_TYPE"

        # tinyint(1) is how mysql represents boolean: https://stackoverflow.com/a/3751882
        query = f"""
        SELECT {col_name_col}, {is_nullable_col},
        IF(COLUMN_TYPE="tinyint(1)", "bool", {data_type_col}) AS {data_type_col}
        FROM information_schema.COLUMNS
        WHERE table_name = '{table_name}'
        """

        df = pd.read_sql_query(query, con=sqlalchemy_engine)

        df_dates = df[df.apply(lambda x: x[data_type_col] == "date", axis=1)]
        date_only_cols = df_dates[col_name_col].tolist()

        df.IS_NULLABLE = df[is_nullable_col].map(
            ElationMetadata.MYSQL_NULLABLE_TO_BQ_MODE
        )
        df.DATA_TYPE = df[data_type_col].map(ElationMetadata.MYSQL_TYPE_TO_BQ_TYPE)

        bq_schema_args = list(
            zip(df[col_name_col], df[data_type_col], df[is_nullable_col])
        )
        bq_schema = [SchemaField(*args) for args in bq_schema_args]

        return date_only_cols, bq_schema


class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class ElationError(Error):
    """Class for throwing/logging of errors related to Elation mirroring.
    Will write to `project.dataset.MIRROR_ERRORS`

    TODO: Consider abstracting into a super class that other mirror jobs can use.
    """

    # Fill below dictionary to temporarily allow tables to be processed without failing
    # on bad records. Make sure to leave a TODO to empty the dictionary once bad records
    # are resolved.

    # TODO: Remove patient_immunization from dictionary once Elation fixes
    # rows with id=276504727257258 and id=276504727257258
    # which have a malformed year of 9999 in administered_date column.
    BAD_RECORDS_ALLOWED = {"patient_immunization": 2}

    ERROR_TABLE_NAME = "MIRROR_ERRORS"

    def __init__(self, table: str, error: str, resource: str, load_time: dt):
        self.table = table
        self.error = error
        self.resource = resource
        self.load_time = load_time

    @staticmethod
    def get_error_bq_schema() -> List[SchemaField]:
        return [
            SchemaField("table", BQHelper.BQ_STRING, BQHelper.BQ_REQUIRED),
            SchemaField("error", BQHelper.BQ_STRING, BQHelper.BQ_REQUIRED),
            SchemaField("resource", BQHelper.BQ_STRING, BQHelper.BQ_REQUIRED),
            SchemaField("load_time", "TIMESTAMP", BQHelper.BQ_REQUIRED),
        ]

    def __str__(self):
        class_name = str(self.__class__.__name__)
        return f"{class_name}: {self.table}\n{self.error}\nResource: {self.resource}\nLoad Time: {self.load_time}\n"
