from argparse import ArgumentParser
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from util.schemas import FRESHNESS_HOURS_TABLE_SCHEMA

logging.getLogger().setLevel(logging.INFO)


class AgeMetric:
    """Wrapper class to compute freshness service level indicators for Cityblock Database mirrors
    Simple counter that just computes an age since the last time this metric was run.
    """
    def __init__(self, db: str, project_id: str, dataset_id: str):
        self.database = db
        self.bq_client = bigquery.Client(project=project_id)

        self.dataset_id = dataset_id
        self.now = datetime.now(timezone.utc)

        self.full_table_id = f"{project_id}.{dataset_id}.{db}_mirror_age"

    def run(self) -> None:
        """Collects the time difference information (diffs) for relevant tables from a conf file (db_ref.json) and then
        uploads this information into GCS. We then get the average of all these diffs to calculate an average age of
        the relevant tables then append a record into a BQ table with information pertaining to this run.
        """

        latest_date = self.now
        for result in self.bq_client.query(f'SELECT MAX(updated_time) AS latest FROM {self.full_table_id}'):
            latest_date = result.get('latest') or latest_date  # first run should go to latter condition

        time_since_last_run = self.now - latest_date.replace(tzinfo=timezone.utc)
        hours_difference = time_since_last_run.seconds / (60 * 60)
        logging.info(f"It has been {hours_difference} hours since the last time this script was run...")
        row = [{'updated_time': self.now, 'hours_since_last_update': round(hours_difference, 2)}]
        self.bq_client.insert_rows(self.full_table_id, row, FRESHNESS_HOURS_TABLE_SCHEMA)
        logging.info(f"Appended SLI update to:{self.full_table_id}")


def _argument_parser():
    arg_parser = ArgumentParser(description="Argument parser for SLO tool for Database mirror freshness metric")
    arg_parser.add_argument("--db", type=str, required=True, help="Which Database to run the freshness metric on")
    arg_parser.add_argument("--project", type=str, required=True, help="GCP Project for BQ clients")
    arg_parser.add_argument("--dataset-id", type=str, default="sli_metrics", help="BQ dataset to store metrics data in")
    return arg_parser


if __name__ == "__main__":
    # Parse input arguments
    args = _argument_parser().parse_args()
    db_arg = args.db
    project_arg = args.project
    dataset_arg = args.dataset_id
    am = AgeMetric(db=db_arg, project_id=project_arg, dataset_id=dataset_arg)
    am.run()
