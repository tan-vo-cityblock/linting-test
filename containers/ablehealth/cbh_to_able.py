import os
import logging
import time
import datetime
import argparse
from typing import List

from google.cloud import bigquery, storage
from google.cloud.bigquery.table import TableReference


class PrepForAble:
    """ Prepares data to be sent to Able Health by running SQL queries on BQ and producing results as CSVs
     and storing them in GCS
    """

    HEADER_SUFFIX = 'header'

    def __init__(self, project_id: str, bucket_name: str, datestamp: str):
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)
        self.datestamp = datestamp

    def run(self) -> None:
        """ 'main' method to run relevant metrics to deliver to Able Health"""
        metrics_to_run = [
            "diagnosis",
            "clinical_activity",
            "laboratory_test",
            "medication",
            "patient",
            "patient_program"
        ]

        for metric in metrics_to_run:
            base_destination_uri = f"able_health_drop/{self.datestamp}/{metric}"
            logging.info(f"Clearing previous files in path {base_destination_uri}, if any, before start of run.")
            self._delete_all_from_path(base_destination_uri)
            self.run_query_and_put_csv_in_gcs(metric, base_destination_uri)

    @staticmethod
    def _delete_blobs(blobs):
        for blob in blobs:
            blob.delete()
            logging.info(f"Deleted blob at {blob.name}.")


    def _delete_all_from_path(self, base_destination_uri):
        all_blobs = list(
            self.storage_client.list_blobs(self.bucket, prefix=base_destination_uri)
        )
        self._delete_blobs(all_blobs)


    def run_query_and_put_csv_in_gcs(self, metric_name: str, base_destination_uri) -> None:
        """ Runs a metric by running a SQL query on BQ then writing those results on GCS as a CSV file
        :param metric_name: name of metric, used as name of CSV and the .sql file to produce that CSV/metric
        :return: Nothing is returned, only the data is written to GCS
        """
        logging.info(f"Running query for {metric_name}")
        q_job = self._stage_query_job(metric_name)
        result = q_job.result()  # API call to BQ that will run the query
        logging.info("Query complete")

        header = [schema_field.name for schema_field in result.schema]
        time.sleep(5)  # delay to ensure the temporary table is available
        self._write_to_gcs(q_job.destination, header, metric_name, base_destination_uri)

    def _stage_query_job(self, metric: str):
        sql = self._retrieve_query_str(metric)
        query_job = self.bq_client.query(sql, location="US")
        return query_job

    def _write_to_gcs(self, query_reference: TableReference, header: List[str], metric_name: str, base_destination_uri):
        logging.info(f"writing {metric_name} results header to GCS")
        header_destination_uri = f"{base_destination_uri}-{self.HEADER_SUFFIX}.csv"
        header_csv_str = ','.join(header) + '\n'
        header_blob = self.bucket.blob(header_destination_uri)
        header_blob.upload_from_string(header_csv_str, content_type='text/csv')
        logging.info(
            f"{metric_name} header now available at {header_destination_uri}")

        logging.info(f"writing {metric_name} results to GCS")
        component_destination_uri = f"gs://{self.bucket.name}/{base_destination_uri}-*.csv"
        extract_job = self.bq_client.extract_table(
            query_reference, component_destination_uri,
            location="US",
            job_config=bigquery.ExtractJobConfig(print_header=False))
        extract_job.result()
        logging.info(
            f"{metric_name} now available as component files with prefix {component_destination_uri}")

        self._compose_in_gcs(metric_name, base_destination_uri)

    def _compose_in_gcs(self, metric_name: str, base_destination_uri: str):
        all_blob_components = list(self.storage_client.list_blobs(
            self.bucket, prefix=base_destination_uri))

        final_uri_ext = f'{base_destination_uri}.csv'

        for blob in all_blob_components:
            if blob.name == final_uri_ext:
                raise ValueError(
                    f'Composite object at "{final_uri_ext}" already exists.')

        header_blob = [
            blob for blob in all_blob_components if self.HEADER_SUFFIX in blob.name]
        body_blobs = [
            blob for blob in all_blob_components if self.HEADER_SUFFIX not in blob.name]
        ordered_blobs = header_blob + body_blobs

        if len(header_blob) != 1:
            raise ValueError(
                f'Found 0 or greater than 1 header files for metric "{metric_name}" in GCS')

        logging.info(f'Composing header and body blobs for {metric_name} with GCS prefix {base_destination_uri}')
        composed_blob = self.bucket.blob(final_uri_ext)
        composed_blob.compose(ordered_blobs)
        logging.info(
            f'Composed {len(ordered_blobs)} blobs for {metric_name} into final composite blob at {final_uri_ext}')

        self._delete_blobs(ordered_blobs)

    @staticmethod
    def _retrieve_query_str(file_name):
        """ Reads the `.sql` file and returns it as a String object"""
        file = os.path.join(os.path.curdir, 'sql', file_name + '.sql')
        fd = open(file)
        sql_str = fd.read()
        fd.close()
        return sql_str


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser for Cityblock data to Able Health")
    cli.add_argument("--project", type=str, required=True,
                     help="BQ project we are reading from")
    cli.add_argument("--gcs-bucket", type=str, required=True,
                     help="Bucket to write AH data to")
    cli.add_argument("--date", type=str,
                     help="Date at which the data is received")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    bq_project = args.project
    gcs_bucket = args.gcs_bucket
    date = args.date or datetime.datetime.utcnow().strftime("%Y-%m-%d")

    prep_for_able = PrepForAble(
        project_id=bq_project, bucket_name=gcs_bucket, datestamp=date)

    prep_for_able.run()
    logging.info(
        f"All CSVs should now be available in bucket: {prep_for_able.bucket.name}/able_health_drop/{date}"
    )
