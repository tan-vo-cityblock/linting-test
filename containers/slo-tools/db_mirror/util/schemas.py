from google.cloud import bigquery

FRESHNESS_TABLE_SCHEMA = [
    bigquery.SchemaField("updated_time", "DATETIME"),
    bigquery.SchemaField("average_age", "FLOAT"),
    bigquery.SchemaField("gcs_url", "STRING")
]

COVERAGE_TABLE_SCHEMA = [
    bigquery.SchemaField("updated_time", "DATETIME"),
    bigquery.SchemaField("shard", "STRING"),
    bigquery.SchemaField("coverage", "FLOAT")
]

FRESHNESS_HOURS_TABLE_SCHEMA = [
    bigquery.SchemaField("updated_time", "DATETIME"),
    bigquery.SchemaField("hours_since_last_update", "FLOAT")
]
