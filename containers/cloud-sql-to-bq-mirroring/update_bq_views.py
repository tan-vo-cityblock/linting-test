import argparse
import datetime
import logging

from google.cloud import bigquery


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for Cloud SQL to GCS dump.")
    cli.add_argument("--project", type=str, required=True, help="GCP project")
    cli.add_argument("--dataset", type=str, required=True, help="BQ dataset")
    cli.add_argument("--tables", type=str, required=True, help="List of tables to update views on")
    cli.add_argument("--date", type=str, help="Shard date that suffix tables, in YYYYMMDD format")
    return cli

# TODO: Use gcp_helper.py's BQ_Helper.update_view_for_latest instead
def update_view(_client: bigquery.Client,
                _dataset_name: str,
                _latest_table_name: str,
                _view_name: str) -> None:

    # Delete existing view for table
    dataset = _client.dataset(_dataset_name)
    old_view_ref = dataset.table(_view_name)
    _client.delete_table(old_view_ref, not_found_ok=True)

    # Recreate view for table using latest shard
    try:
        view_ref = dataset.table(_view_name)
        view = bigquery.Table(view_ref)
        view.view_query = f"select * from `{_latest_table_name}`"
        _client.create_table(view)
    except BaseException as e:
        logging.error(f"Failed to create view {_view_name} for shard {_latest_table_name} [error: {e}].")
        exit(1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    project = args.project
    dataset_name = args.dataset
    table_names = args.tables.split(',')
    date = args.date or datetime.datetime.utcnow().strftime("%Y%m%d")

    # Open client and construct reference to dataset
    client = bigquery.Client(project=project)

    # Update each view passed in as an argument
    for table_name in table_names:
        latest_table_name = f"{project}.{dataset_name}.{table_name}_{date}"
        update_view(client, dataset_name, latest_table_name, table_name)

    # Close connection to BQ
    client.close()
