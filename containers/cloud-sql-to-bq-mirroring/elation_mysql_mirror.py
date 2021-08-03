import argparse
import logging
import os
from datetime import datetime as dt
from time import sleep
from typing import List

import pandas as pd
from google.cloud.bigquery import SchemaField
from pytz import utc
from sqlalchemy import create_engine
from sshtunnel import (
    BaseSSHTunnelForwarderError,
    HandlerSSHTunnelForwarderError,
    SSHTunnelForwarder,
)

from elation_mirror_helper import ElationError, ElationMetadata, ElationSecret
from gcp_helper import NULL_MARKER, BQHelper, StorageHelper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElationMySQLMirror:
    """Class for mirroring tables in our instance of the Elation Hosted DB into GCS and BQ.

    # TODO Trim the below and link to README

    Overview of procedures executed:
    1. Use 'sshtunnel' package to ssh into Elation server using ElationSecret property.
    2. Use the 'sqlalchemy' package to connect to the hosted database via SSH tunnel.
    3. In Elation, use ElationMetadata static class to query information_schema to generate metadata
    regarding tables to be downloaded and the timezone of the DB. Write to GCS -> BQ
    4. Loop through list of tables in metadata and download the full table
    along with column mode data (is_nullable) from information_schema.COLUMNS. Write to GCS -> BQ
    5. Catch errors related to writing to GCS->BQ and store in array of errors of type ElationErrors.
    6. If errors array is not empty, write errors to GCS->BQ, and throw final uncaught exception to fail the job.
    Note this means that the job run to completion as long as it can catch errors it's designed to even if certains tables don't get written to GCS->BQ.
    All other uncaught errors will fail the job and should occur before or after all tables are loaded.

    Overview of BQ Load:
    - The BQ load job is currently configured to use partitioning and breaks convention by not using table sharding.
    - Partioning is recommened by GCP for its efficiencies, see more here: https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables
    - The partition dates for the MIRROR_METADATA and MIRROR_ERRORS are set to dt.now(utc) representing the current run.
    - The partition dates for mirrored Elation tables are set to the 'CREATE_TIME' of each table representing the date Elation last refreshed the table.
    - Each partitioned table is set to overwrite if the job is re-run for a given partition date.

    Improvements:
    - Investigate parallelizing steps above, i.e multiple connections to server, loads to gcs, jobs for GCS->BQ.
    - Monitor 'modin[dask].pandas' package (parallelize pandas) to see if you can use it on pd.read_sql calls
    that need to happen via a connection engine b/c Elation host requires dummy ssl (aka ssl without actual certs).
    Right now, modin can only be used via connection strings which can't take dummy ssl values.
    """

    # Max memory size used to calculate how many rows to read/upload at a time from MySQL table.
    # TODO: See if this can be dynamically set by taking into account compute machine limits
    MAX_CHUNK_SIZE_MB = 50

    MAX_SERVER_CONNECTION_RETRY = 5

    def __init__(
        self,
        project: str,
        dataset: str,
        bucket: str,
        elation_mirror_secret: ElationSecret,
    ):
        self.project = project
        self.dataset = dataset
        self.bucket = bucket
        self.secret = elation_mirror_secret
        self.load_time = dt.now(utc)
        self.server = SSHTunnelForwarder(
            ssh_address_or_host=self.secret.ssh_hostname,
            ssh_username=self.secret.ssh_user,
            ssh_pkey=self.secret.ssh_private_key_path,
            remote_bind_address=(
                self.secret.mysql_remote_host,
                self.secret.mysql_remote_port,
            ),
        )
        # https://github.com/pahaz/sshtunnel/issues/107#issuecomment-347687937
        self.server.skip_tunnel_checkup = False
        # see for more: https://github.com/pahaz/sshtunnel/issues/72#issuecomment-573138507
        self.server.daemon_forward_servers = True

        self.bq_helper = BQHelper(self.project)
        self.storage_helper = StorageHelper(self.project)
        self.errors = []

    def _mk_mysql_con_uri(self, local_port: str) -> str:
        mysql_user = self.secret.mysql_user
        mysql_password = self.secret.mysql_password
        mysql_host = self.secret.mysql_local_host
        mysql_database = self.secret.mysql_database

        return f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{local_port}/{mysql_database}"

    def _mk_load_time_for_gcs_path(self) -> str:
        return self.load_time.strftime("%Y%m%d%H%M%S")

    def _mk_parition_from_load_time(self) -> str:
        return self.load_time.strftime("%Y%m%d")

    def _mk_gcs_uri(
        self, table_name: str, partition: str, chunk_count: int = None
    ) -> str:
        load_time_path = self._mk_load_time_for_gcs_path()
        gcs_uri = f"elation_db/{load_time_path}/{table_name}_{partition}"
        if chunk_count is not None:
            gcs_uri = f"{gcs_uri}_{chunk_count}"
        return gcs_uri

    def _recast_nullable_int_cols(
        self, df: pd.DataFrame, bq_schema: List[SchemaField]
    ) -> pd.DataFrame:
        # below done to fix pd.read_sql converting nullable int columns in elation db to float in pandas
        # Fix by changing float columns to pd.Int64Dtype() which pandas introduced for Nullable integer data type
        # See more here: https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        # And here: https://stackoverflow.com/questions/37796916/pandas-read-sql-integer-became-float

        def should_recast(col: SchemaField) -> bool:
            return col.mode == BQHelper.BQ_NULLABLE and (
                col.field_type == BQHelper.BQ_INTEGER
                or col.field_type == BQHelper.BQ_BOOL
            )

        cols_to_recast = [col.name for col in bq_schema if should_recast(col)]
        if cols_to_recast:
            logger.info(
                f'Recasting nullable int columns {cols_to_recast} from "np.float64" to "pd.Int64Dtype"'
            )
            df[cols_to_recast] = df[cols_to_recast].astype(pd.Int64Dtype())

        return df

    @staticmethod
    def _recast_date_only_cols(
        date_only_cols: List[str], df: pd.DataFrame
    ) -> pd.DataFrame:
        # Recasting date only columns in pandas from datetime[ns] to str object with format '%Y-%m-%d'
        # so it writes to csv as a date and can be uploaded to BQ as a DATE type
        for col in date_only_cols:
            df[col] = df[col].dt.strftime("%Y-%m-%d")

        return df

    @staticmethod
    def _get_rows_chunk_size(row_count: int, row_size_mb: int) -> int:
        if row_count == 0 or row_size_mb == 0:
            return row_count
        size_per_row = row_size_mb / float(row_count)
        return int(ElationMySQLMirror.MAX_CHUNK_SIZE_MB / size_per_row)

    def _persist_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        table_name = ElationMetadata.METADATA_TABLE_NAME
        schema = ElationMetadata.get_metadata_bq_schema()
        partition = self._mk_parition_from_load_time()
        try:
            full_gcs_uri = self._write_df_to_gcs(
                table_name=table_name, partition=partition, df=df
            )

            self._write_gcs_to_bq(
                table_name=table_name,
                partition=partition,
                full_gcs_uri=full_gcs_uri,
                schema=schema,
            )
        except ElationError as err:
            self.errors.append(err)

    def _process_and_persist_table(self, engine, tables_metadata: pd.DataFrame):

        for row in tables_metadata.itertuples():
            table_name = row.TABLE_NAME
            partition = row.BQ_PARTITION
            table_rows_count = row.TABLE_ROWS

            if table_rows_count == 0:
                logger.info(
                    f'Skipping proccesing of empty Elation table: "{table_name}"\n'
                )
                continue

            chunk_size_rows = self._get_rows_chunk_size(
                table_rows_count, row.TABLE_SIZE_MB
            )

            date_only_cols, bq_schema = ElationMetadata.get_table_schema_info(
                sqlalchemy_engine=engine, table_name=table_name
            )

            chunk_count = 0

            for table_df_chunk in pd.read_sql_table(
                table_name, con=engine, chunksize=chunk_size_rows
            ):
                logger.info(
                    f'Downloaded Elation table chunk: "{table_name}_{chunk_count}" with row count {len(table_df_chunk)} from Elation server to Pandas.'
                )

                table_df_chunk_recasted = self._recast_nullable_int_cols(
                    df=table_df_chunk, bq_schema=bq_schema
                )
                table_df_chunk_recasted = self._recast_date_only_cols(
                    date_only_cols=date_only_cols, df=table_df_chunk_recasted
                )

                include_header = chunk_count == 0

                self._write_table_chunk_to_gcs(
                    table_name=table_name,
                    partition=partition,
                    chunk_count=chunk_count,
                    df=table_df_chunk_recasted,
                    include_header=include_header,
                )

                chunk_count = chunk_count + 1

            self._persist_db_table(
                table_name=table_name, partition=partition, schema=bq_schema
            )

    def _write_table_chunk_to_gcs(
        self,
        table_name: str,
        partition: str,
        chunk_count: int,
        df: pd.DataFrame,
        include_header: bool,
    ):
        try:
            self._write_df_to_gcs(
                table_name=table_name,
                partition=partition,
                chunk_count=chunk_count,
                df=df,
                include_header=include_header,
            )
        except ElationError as err:
            self.errors.append(err)

    def _persist_db_table(
        self,
        table_name: str,
        partition: str,
        schema: List[SchemaField],
        df: pd.DataFrame = None,
    ):

        try:
            gcs_uri_chunk_prefix = self._mk_gcs_uri(
                table_name=table_name, partition=partition
            )

            full_gcs_uri = self.storage_helper.compose_csv_objects(
                bucket_name=self.bucket, gcs_uri_chunk_prefix=gcs_uri_chunk_prefix
            )

            self._write_gcs_to_bq(
                table_name=table_name,
                partition=partition,
                full_gcs_uri=full_gcs_uri,
                schema=schema,
            )
        except ElationError as err:
            self.errors.append(err)

    def _persist_errors(self):
        """Do not catch errors from writing to GCS->BQ here otherwise will lead to infinite/recursive loop."""
        table_name = ElationError.ERROR_TABLE_NAME
        schema = ElationError.get_error_bq_schema()
        errors_df = pd.DataFrame([vars(e) for e in self.errors])
        partition = self._mk_parition_from_load_time()

        full_gcs_uri = self._write_df_to_gcs(
            table_name=table_name, partition=partition, df=errors_df
        )

        self._write_gcs_to_bq(
            table_name=table_name,
            partition=partition,
            full_gcs_uri=full_gcs_uri,
            schema=schema,
        )

    def _write_df_to_gcs(
        self,
        table_name: str,
        partition: str,
        df: pd.DataFrame,
        chunk_count: int = None,
        include_header: bool = True,
    ) -> str:
        gcs_uri = self._mk_gcs_uri(table_name, partition, chunk_count)

        try:
            table_name_for_log = (
                f'"{table_name}"'
                if chunk_count is None
                else f'chunk "{table_name}_{chunk_count}"'
            )
            logger.info(
                f'Uploading table: {table_name_for_log} to GCS bucket "{self.bucket}" at path "{gcs_uri}"'
            )
            return self.storage_helper.upload_df_as_csv(
                bucket_name=self.bucket,
                gcs_uri=gcs_uri,
                df=df,
                null_marker=NULL_MARKER,
                include_header=include_header,
            )
        except Exception as ex:
            logger.error(f'Failed to upload table "{table_name}" to GCS.\nError: {ex}')
            raise ElationError(
                table=table_name,
                error=f"{ex}",
                resource="GCS",
                load_time=self.load_time,
            )

    def _write_gcs_to_bq(
        self,
        table_name: str,
        partition: str,
        full_gcs_uri: str,
        schema: List[SchemaField],
    ):
        try:
            logger.info(
                f'Loading "{full_gcs_uri}" to BQ table "{table_name}" for partition "{partition}"'
            )

            max_bad_records = ElationError.BAD_RECORDS_ALLOWED.get(table_name, 0)
            self.bq_helper.load_from_gcs(
                gcs_uri=full_gcs_uri,
                dataset_name=self.dataset,
                table_name=table_name,
                partition_date=partition,
                schema=schema,
                for_csv=True,
                allow_overwrite=True,
                allow_schema_updates=True,
                max_bad_records=max_bad_records,
            )

            logger.info(
                f'Creating latest BQ view from "{table_name}" for partition "{partition}"\n'
            )

            self.bq_helper.update_view_for_latest(
                dataset_name=self.dataset,
                table_name=table_name,
                partition_date=partition,
                allow_overwrite=True,
            )
        except Exception as ex:
            logger.error(f'Failed to write table "{table_name}" to BQ.\nError: {ex}')
            raise ElationError(
                table=table_name, error=f"{ex}", resource="BQ", load_time=self.load_time
            )

    def _start_server_and_get_engine(self):
        for i in range(self.MAX_SERVER_CONNECTION_RETRY):
            logger.info(
                f"Server connection attempt {i+1} of {self.MAX_SERVER_CONNECTION_RETRY}."
            )
            try:
                self.server.start()
                logger.info("Successfully connected to and started Elation SSH Server")
                break
            except (BaseSSHTunnelForwarderError, HandlerSSHTunnelForwarderError) as e:
                logger.error(
                    f"Failed to connect to and start Elation Server. Error {e}\n"
                )
                self.shutdown_engine_and_server()
                # Add arbitrary sleep to be safe and not spam connection attempts.
                sleep(10)
                if i == self.MAX_SERVER_CONNECTION_RETRY - 1:
                    raise

        engine = None
        try:
            logger.info("Creating Elation MySQL engine")
            local_port = str(self.server.local_bind_port)
            con_url = self._mk_mysql_con_uri(local_port=local_port)

            # set server_side_cursors=True to enable proper chunking by pandas.read_sql
            # see for more on chunking: https://github.com/pandas-dev/pandas/issues/12265#issuecomment-311623701
            # dummy ssl dict required b/c Elation requires ssl connection but does not require actual ssl certificates.
            engine = create_engine(
                con_url,
                pool_pre_ping=True,
                server_side_cursors=True,
                connect_args={"ssl": {"dummy_ssl": True}},
            )
            logger.info("Successfully created MySQL engine\n")
            return engine
        except Exception as e:
            logger.error(f"Failed to create sqlalchemy mysql engine. Error {e}")
            self.shutdown_engine_and_server(engine)
            raise

    def shutdown_engine_and_server(self, sqlalchemy_engine=None):
        if sqlalchemy_engine:
            sqlalchemy_engine.dispose()
            logger.info("Stopped MySQL Engine")
        self.server.stop()
        logger.info("Stopped SSH Server")

    def run_mirror(self):
        logger.info("Running Elation Mirror...")
        engine = self._start_server_and_get_engine()

        logger.info(
            f'Generating "{ElationMetadata.METADATA_TABLE_NAME}" from Elation database'
        )
        tables_metadata = ElationMetadata.get_metadata_df(engine, self.load_time)
        self._persist_metadata(tables_metadata)
        self._process_and_persist_table(engine=engine, tables_metadata=tables_metadata)

        self.shutdown_engine_and_server(engine)

        if self.errors:
            # TODO: See if we can setup alerting for caught errors without failing the DAG by raising this exception on purpose.
            self._persist_errors()
            for error in self.errors:
                logger.error(error)
            raise RuntimeError(
                "FAILURE: Errors were recorded during running of mirror. See logs above."
            )


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser for Cloud SQL to GCS dump."
    )
    cli.add_argument(
        "--project", type=str, required=True, help="Project we are exporting to"
    )
    cli.add_argument(
        "--dataset", type=str, required=True, help="BQ Dataset we are exporting to"
    )
    cli.add_argument(
        "--bucket", type=str, required=True, help="GCS bucket we are exporting to"
    )
    cli.add_argument(
        "--elation_secret_path",
        type=str,
        default=os.environ.get("ELATION_SECRET_PATH"),
        help="Path to json file holding secrets",
    )

    return cli


if __name__ == "__main__":
    args = argument_parser().parse_args()

    elation_mirror_secret = ElationSecret.from_json_file(args.elation_secret_path)

    mysql_elation_mirror = ElationMySQLMirror(
        project=args.project,
        dataset=args.dataset,
        bucket=args.bucket,
        elation_mirror_secret=elation_mirror_secret,
    )

    mysql_elation_mirror.run_mirror()
    logger.info("SUCCESS: Elation Mirror run complete")
