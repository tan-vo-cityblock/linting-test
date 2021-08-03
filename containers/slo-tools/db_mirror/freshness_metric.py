from argparse import ArgumentParser
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from google.cloud import bigquery
from google.cloud import storage

from util.schemas import FRESHNESS_TABLE_SCHEMA

logging.getLogger().setLevel(logging.INFO)


class FreshnessMetric:
    """Wrapper class to compute freshness service level indicators for Cityblock Database mirrors

    """
    def __init__(self, db: str, project_id: str, metrics_bucket: str, dataset_id: str):
        self.database = db
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)

        self.metrics_bucket = metrics_bucket
        self.dataset_id = dataset_id
        self.now = datetime.now(timezone.utc)

        self.db_reference = self._load_db_reference()
        self.full_table_id = f"{project_id}.{dataset_id}.{db}_mirror_freshness"

    def _load_db_reference(self) -> Dict:
        with open('/db_mirror/util/db_ref.json') as f:
            try:
                mirror_config = json.load(f)[self.database]
            except KeyError:
                logging.error(f"Can't find reference for mirror: [{self.database}] in db_ref.json!")
                raise
        return mirror_config

    def _upload_table_data_to_gcs(self, data: List[str]) -> str:
        bucket = self.storage_client.get_bucket(self.metrics_bucket)
        blob = bucket.blob(f'{self.database}_mirror/freshness/{self.now.strftime("%Y%m%dT%H%M%S%Z")}.csv')
        blob.upload_from_string("\n".join(data))
        return blob.public_url

    def _insert_record_into_bq(self, avg_age: float, tables_url: str):
        row = [
            {'updated_time': self.now, 'average_age': round(avg_age, 2), 'gcs_url': tables_url}
        ]
        self.bq_client.insert_rows(self.full_table_id, row, FRESHNESS_TABLE_SCHEMA)

    def _ignore_table(self, table):
        not_a_table = table.table_type != 'TABLE'
        non_applicable_table = table.table_id in self.db_reference['tables_to_skip']
        return not_a_table or non_applicable_table

    def run(self) -> None:
        """Collects the time difference information (diffs) for relevant tables from a conf file (db_ref.json) and then
        uploads this information into GCS. We then get the average of all these diffs to calculate an average age of
        the relevant tables then append a record into a BQ table with information pertaining to this run.

        """
        gcs_csv_to_upload = ['full_table_id,updated_at']

        diffs = []
        logging.info(f"Collecting age diff info for {self.database}...")
        for dataset_item in self.db_reference['datasets_to_inspect']:
            tables_list = self.bq_client.list_tables(dataset_item)

            for table_item in tables_list:
                table = self.bq_client.get_table(table_item.reference)
                if self._ignore_table(table):
                    continue
                diff = self.now - table.modified
                diffs.append(diff)
                gcs_csv_to_upload.append(f'{table.full_table_id},{table.modified}')

        tables_url = self._upload_table_data_to_gcs(gcs_csv_to_upload)
        logging.info(f"Uploaded detailed diff report to: {tables_url}")

        avg_age = sum(diffs, timedelta(0)) / len(diffs)
        avg_age_hours = avg_age.total_seconds() / 3600
        self._insert_record_into_bq(avg_age_hours, tables_url)
        logging.info(f"Appended SLI update to:{self.full_table_id}")


def _argument_parser():
    arg_parser = ArgumentParser(description="Argument parser for SLO tool for Database mirror freshness metric")
    arg_parser.add_argument("--db", type=str, required=True, help="Which Database to run the freshness metric on")
    arg_parser.add_argument("--project", type=str, required=True, help="GCP Project for BQ and GCS clients")
    arg_parser.add_argument("--bucket", type=str, required=True, help="GCP Bucket to store metrics data in")
    arg_parser.add_argument("--dataset-id", type=str, default="sli_metrics", help="BQ dataset to store metrics data in")
    return arg_parser


if __name__ == "__main__":
    # Parse input arguments
    args = _argument_parser().parse_args()
    db_arg = args.db
    project_arg = args.project
    bucket_arg = args.bucket
    dataset_arg = args.dataset_id
    fm = FreshnessMetric(db=db_arg, project_id=project_arg, metrics_bucket=bucket_arg, dataset_id=dataset_arg)
    fm.run()
