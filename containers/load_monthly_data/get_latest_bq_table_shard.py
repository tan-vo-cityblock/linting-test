import itertools
import logging
import re
from argparse import ArgumentParser
from typing import List, Optional

from google.cloud import bigquery

import common_utils


class LatestBQTableShard:

    def __init__(self, project: str):
        self.client = bigquery.Client(project=project)

    @staticmethod
    def _table_name_has_shard(name: str):
        return len(re.findall("(20[0-9]{6})", name)) > 0

    @staticmethod
    def _get_shard_from_table_name(name: str) -> Optional[str]:
        """Extracts date shard suffix from BQ table name."""
        matches = re.findall("_(20[0-9]{6})$", name)
        if len(matches) < 1:
            return None
        return matches[0]

    @staticmethod
    def _get_prefix_from_table_name(name: str) -> str:
        return re.findall("^(.*)_20[0-9]{6}$", name)[0]

    @staticmethod
    def _determine_latest_shard(table_ids: List[str], required_tables: List[str]) -> str:
        tables_with_date = map(
            lambda t: (LatestBQTableShard._get_shard_from_table_name(t), t),
            table_ids)
        tables_by_date_descending = list(sorted(tables_with_date, key=lambda x: x[0], reverse=True))
        for date, tables in itertools.groupby(tables_by_date_descending, lambda x: x[0]):
            tables_present = list(filter(
                lambda t: LatestBQTableShard._get_prefix_from_table_name(t[1]) in required_tables,
                tables))

            if len(tables_present) == len(required_tables):
                return date
        raise RuntimeError("No date found with all tables present")

    def run(self, dataset: str, required_tables: List[str]) -> str:
        """Returns latest date shard for which all given tables exist in the given dataset."""
        tables_in_dataset = [t.table_id for t in self.client.list_tables(dataset=dataset) if
                             self._table_name_has_shard(t.table_id)]
        return self._determine_latest_shard(tables_in_dataset, required_tables)


def _argparser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--required_tables', required=True)
    return parser


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    args = _argparser().parse_args()
    project = args.project
    dataset = args.dataset
    required_tables = args.required_tables.split(',')

    date = LatestBQTableShard(project=project).run(dataset=dataset, required_tables=required_tables)
    logging.info(f"Found most recent date {date} with all tables present {required_tables}")

    common_utils.write_str_to_sidecar(date)
