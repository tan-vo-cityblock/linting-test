import argparse

from google.cloud import bigquery

import common_utils


class BigQueryShard:
    """ Uses Google Cloud BigQuery API to get latest shard (date) for a given partner data"""

    def __init__(self, project: str):
        self.client = bigquery.Client(project=project)

    def latest_shard_exist(self, dataset: str, required_tables: str,
                           latest_known_shard: str) -> bool:
        """ Checks if the latest shard date is available in the tables for the relevant dataset from partner project.
        We first filter for tables that contain the shard in the dataset. Then we match against the required tables
         and ensure they are all present. If so, we can definitively state the latest shard exists.

        Args:
            dataset: The BigQuery DataSet that belongs to self.project for which to look up the latest shard
            required_tables: Tables required to have the shard date
            latest_known_shard: This sharded date is used in order to compare against the BigQuery tables within the provided dataset

        Returns:
            whether or not the latest shard is found with respect to the DataSet provided

        """
        # parsing the raw string into a list of strings (tables)
        clean_req_tables = required_tables.split(",")
        tables = self.client.list_tables(dataset)
        table_names = map(lambda table: table.full_table_id, tables)
        tables_with_date = filter(lambda t: latest_known_shard in t, table_names)

        filtered_tables = []
        for bq_table in tables_with_date:
            only_table_name = bq_table.split(".")[-1]
            for req_table in clean_req_tables:
                if f"{req_table}_{latest_known_shard}" == only_table_name:
                    filtered_tables.append(bq_table)

        return len(filtered_tables) == len(clean_req_tables)


def setup_argparser():
    cli = argparse.ArgumentParser()
    cli.add_argument(
        "--project",
        type=str,
    )
    cli.add_argument(
        "--dataset",
        type=str,
    )
    cli.add_argument(
        "--tables",
        type=str,
    )
    cli.add_argument(
        "--date",
        type=str,
    )

    return cli


if __name__ == "__main__":
    args = setup_argparser().parse_args()
    bq_shard = BigQueryShard(project=args.project)
    found_table = bq_shard.latest_shard_exist(dataset=args.dataset, required_tables=args.tables,
                                              latest_known_shard=args.date)
    common_utils.write_str_to_sidecar(str(found_table))
