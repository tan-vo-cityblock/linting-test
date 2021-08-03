import argparse
import datetime
import subprocess
import logging


class CloudSqlToGcsExport:
    """ Class encapsulating Cloud SQL exports to GCS through the gcloud CLI """

    def __init__(self, project_name: str, instance_name: str, database_name: str, bucket_name: str):
        self.project_name = project_name
        self.instance_name = instance_name
        self.database_name = database_name
        self.bucket_name = bucket_name

    @staticmethod
    def gcloud_auth(key_file_path: str) -> None:
        """
        Method used to authenticate a service account to run the export commands. Done here instead of on the Docker
        container as the secrets get mounted after the container gets built.
        @param key_file_path: location of secrets to authenticate with
        """
        cmd = f"gcloud auth activate-service-account --key-file={key_file_path}"
        subprocess.check_call(cmd, shell=True)

    def export_file(self, file_name: str, query: str) -> None:
        """
        Method used to execute a CSV dump of a database on a Cloud SQL instance to a GCS location.
        @param file_name: name of the export file
        @param query:     query executed on CSV dump
        """
        export_location = f"gs://{self.bucket_name}/{file_name}"
        cmd = f"""gcloud sql export csv {self.instance_name} {export_location} --project {self.project_name} --database={self.database_name} --query="{query}" """
        subprocess.check_call(cmd, shell=True)

    def export_data_file(self, table_name: str, file_name: str) -> None:
        query = f"SELECT * FROM {table_name}"
        try:
            self.export_file(file_name, query)
        except subprocess.CalledProcessError:
            logging.error(
                "Error running data export to GCS "
                f"[instance: {self.instance_name}, database: {self.database_name}, table: {table_name}]"
            )
            exit(1)

    def export_schema_file(self, table_name: str, file_name: str) -> None:
        query = f"""SELECT column_name, data_type, is_nullable
                 FROM information_schema.columns
                 WHERE table_name = '{table_name}'
                 ORDER BY ORDINAL_POSITION"""
        try:
            self.export_file(file_name, query)
        except subprocess.CalledProcessError:
            logging.error(
                "Error running schema export to GCS "
                f"[instance: {self.instance_name}, database: {self.database_name}, table: {table_name}]"
            )
            exit(1)


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for Cloud SQL to GCS dump.")
    cli.add_argument("--project", type=str, required=True, help="BQ project we are exporting to")
    cli.add_argument("--instance", type=str, required=True, help="Cloud SQL instance name")
    cli.add_argument("--database", type=str, required=True, help="Name of database to export from")
    cli.add_argument("--tables", type=str, required=True, help="Names of tables to export")
    cli.add_argument("--output-bucket", type=str, required=True, help="GCS bucket we are exporting to")
    cli.add_argument("--key-path", type=str, required=True, help="Path to credentials for GC Service account")
    cli.add_argument("--date", type=str, help="Shard date to suffix export files")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    bq_project = args.project
    cloud_sql_instance = args.instance
    database = args.database
    tables = args.tables
    output_bucket_name = args.output_bucket
    date = args.date or datetime.datetime.utcnow().strftime("%Y%m%d")

    # Initialize exporter class
    cloud_sql_to_gcs_export = CloudSqlToGcsExport(
        project_name=bq_project,
        instance_name=cloud_sql_instance,
        database_name=database,
        bucket_name=output_bucket_name
    )
    logging.info(f"Created CloudSqlToGcsExport instance [project: {bq_project}, instance: {cloud_sql_instance}]")
    logging.info(f"CloudSqlToGcsExport input configured as [database: {database}, tables: {tables}]")
    logging.info(f"CloudSqlToGcsExport output configured as [output_bucket: {output_bucket_name}]")

    # gcloud authentication to make cloud sql calls
    key_path = args.key_path
    cloud_sql_to_gcs_export.gcloud_auth(key_path)

    # For each table, export data and schema to GCS
    for table in map(str.strip, tables.split(',')):
        data_file_name = f"{database}_export_{table}_data_{date}"
        schema_file_name = f"{database}_export_{table}_schema_{date}"

        logging.info(f"Exporting data file [table: {table}, file_name: {data_file_name}]")
        cloud_sql_to_gcs_export.export_data_file(table, data_file_name)

        logging.info(f"Exporting schema file [table: {table}, file_name: {schema_file_name}]")
        cloud_sql_to_gcs_export.export_schema_file(table, schema_file_name)

    logging.info(f"Finished Cloud SQL export for [database: {database}, tables: {tables}]")
