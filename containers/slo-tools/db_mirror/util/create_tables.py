from google.cloud import bigquery

from schemas import FRESHNESS_TABLE_SCHEMA, COVERAGE_TABLE_SCHEMA, FRESHNESS_HOURS_TABLE_SCHEMA


def create_freshness_table(project: str, dataset: str, database: str):
    bq_client = bigquery.Client(project=project)
    table = bigquery.Table(f'{project}.{dataset}.{database}_mirror_freshness', schema=FRESHNESS_TABLE_SCHEMA)
    bq_client.create_table(table)


def create_coverage_table(project: str, dataset: str, database: str):
    bq_client = bigquery.Client(project=project)
    table = bigquery.Table(f'{project}.{dataset}.{database}_mirror_coverage', schema=COVERAGE_TABLE_SCHEMA)
    bq_client.create_table(table)


def create_freshness_age_table(project: str, dataset: str, database: str):
    bq_client = bigquery.Client(project=project)
    table = bigquery.Table(f'{project}.{dataset}.{database}_mirror_age', schema=FRESHNESS_HOURS_TABLE_SCHEMA)
    bq_client.create_table(table)
