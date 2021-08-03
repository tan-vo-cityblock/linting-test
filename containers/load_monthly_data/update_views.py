import sys
from google.cloud import bigquery
import argparse
import logging

def argument_parser():
   cli = argparse.ArgumentParser(description="Argument parser for Cloud SQL to GCS dump.")
   cli.add_argument("--project", type=str, required=True, help="BQ project we are working with")
   cli.add_argument("--view_dataset", type=str, required=False, help="Dataset containing view if different from dataset that table is pointing at")
   cli.add_argument("--dataset", type=str, required=True, help="Dataset we are working with")
   cli.add_argument("--views", type=str, required=True, help="List of views to modify")
   cli.add_argument("--date", type=str, required=True, help="Shard date that suffix tables, in YYYYMMDD format")
   return cli

def generate_new_sql(date: str, project: str, dataset: str, table: str):
    """ Generates a view's SQL to pull everything from the given table.
    Assumes that the view is always select * from {table}
    Assumes that the table will always exist with a name of {view name}_{date, always passed in with YYYYMMDD format}"""

    latest_table = f"{project}.{dataset}.{table}_{date}"
    sqlQuery = f"select * from `{latest_table}`"

    return sqlQuery

def update_view(view_name: str, dataset_ref, client, sql_query: str):
    """ Updates the given view with the given SQL query, using client connection.
    Creates a view reference within the given dataset reference, assigns it to the BQ type Table and updates its
    view_query property."""
    view_ref = dataset_ref.table(view_name)
    view = bigquery.Table(view_ref)
    view.view_query = sql_query # Sets view_query to given SQL string
    client.update_table(view, ["view_query"]) # API request

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    bq_project = args.project
    dataset = args.dataset
    views = args.views.split(',')
    date = args.date
    view_dataset = args.view_dataset or dataset

    # Open client and construct reference to dataset
    client = bigquery.Client(project = bq_project)
    dataset_ref = client.dataset(view_dataset)

    # Update each view passed in as an argument
    for view in views:
        new_sql = generate_new_sql(date, bq_project, dataset, view) # Create new SQL statement
        update_view(view, dataset_ref, client, new_sql) # Update view's SQL
        logging.info(f'Updated view {bq_project}:{view_dataset}.{view} with SQL: "{new_sql}"')

    # Close connection to BQ
    client.close()


