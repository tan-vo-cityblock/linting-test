from argparse import ArgumentParser
from datetime import datetime, timezone
import logging
from typing import Dict

from google.cloud import bigquery

from util.schemas import COVERAGE_TABLE_SCHEMA

logging.getLogger().setLevel(logging.INFO)


class CoverageMetric:
    """Wrapper class to compute coverage service level indicators for Cityblock Database mirrors

    """
    def __init__(self, db: str, project_id: str, shard: str, dataset_id: str, mirror_project: str):
        self.database = db
        self.bq_client = bigquery.Client(project=project_id)

        self.date_shard = shard
        self.mirror_project = mirror_project
        self.now = datetime.now(timezone.utc)
        self.full_table_id = f"{project_id}.{dataset_id}.{db}_mirror_coverage"

    def _insert_record_into_bq(self, coverage: float):
        row = [
            {'updated_time': self.now, 'shard': self.date_shard, 'coverage': round(coverage, 4)}
        ]
        self.bq_client.insert_rows(self.full_table_id, row, COVERAGE_TABLE_SCHEMA)

    def see_error_summary(self) -> Dict:
        """For a given shard find the counts of the error rows then return a Dict containing coverage information
        for all the tables that do not have 100% coverage

        """
        errors = self.bq_client.query(f"""
            SELECT tableName, COUNT(*) AS skipped_rows 
            FROM `{self.mirror_project}.{self.database}_mirror.{self.database}_mirror_errors_{self.date_shard}`
            GROUP BY tableName
        """)
        error_rows_map = {}
        for row in errors:  # force query to run and then iterate over the results
            error_rows_map[row.get('tableName')] = row.get('skipped_rows')

        valid_rows_map = {}
        for table in error_rows_map.keys():
            table_count = self.bq_client.query(f"""
                SELECT COUNT(*) AS total 
                FROM `{self.mirror_project}.{self.database}_mirror.{table}_{self.date_shard}`
            """)
            for count in table_count:  # force query to run and then iterate over the results
                valid_rows_map[table] = count.get('total')

        coverage_map = {}
        for table, processed_rows in valid_rows_map.items():
            skipped_rows = error_rows_map[table]
            coverage_map[table] = round((processed_rows / (processed_rows + skipped_rows)) * 100, 4)

        return coverage_map

    def run(self):
        """Runs a query on BQ using meta data on row counts for a given shard then calculates a coverage percentage
        which is then appended to separate table on BQ

        """
        coverage_table = self.bq_client.query(f"""
            WITH valid_total AS (
                SELECT SUM(row_count) AS all_rows 
                FROM `{self.mirror_project}.{self.database}_mirror.__TABLES__` 
                WHERE ends_with(table_id, '{self.date_shard}')
            ), error_total AS (
                SELECT COUNT(*) AS error_rows 
                FROM `{self.mirror_project}.{self.database}_mirror.{self.database}_mirror_errors_{self.date_shard}`
            )
            SELECT (valid_total.all_rows / (valid_total.all_rows + error_total.error_rows) * 100) AS coverage
            FROM valid_total, error_total
            """)
        for coverage in coverage_table:  # force query to run and then iterate over the results (only one)
            self._insert_record_into_bq(coverage['coverage'])
            logging.info(f"Appended SLI update to:{self.full_table_id}")


def _argument_parser():
    arg_parser = ArgumentParser(description="Argument parser for SLO tool for Database mirror coverage metric")
    arg_parser.add_argument("--db", type=str, required=True, help="Which Database to run the freshness check on")
    arg_parser.add_argument("--project", type=str, required=True, help="GCP Project for BQ client")
    arg_parser.add_argument("--shard", type=str, required=True, help="Date shard to get coverage for")
    arg_parser.add_argument("--dataset-id", type=str, default="sli_metrics", help="GCP Bucket to store metrics data in")
    arg_parser.add_argument("--mirror-project", type=str, default="cbh-db-mirror-prod", help="Project containg Database"
                                                                                             "mirror data")
    return arg_parser


if __name__ == "__main__":
    # Parse input arguments
    args = _argument_parser().parse_args()
    db_arg = args.db
    project_arg = args.project
    shard_arg = args.shard or datetime.utcnow().strftime("%Y%m%d")
    dataset_arg = args.dataset_id
    mirror_project_arg = args.mirror_project
    cm = CoverageMetric(db=db_arg, project_id=project_arg, shard=shard_arg, dataset_id=dataset_arg,
                        mirror_project=mirror_project_arg)
    cm.run()
