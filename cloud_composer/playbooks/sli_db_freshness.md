# SLI for Database Freshness
Service Level Indicator for freshness metric on Database Mirror ETLs

## What is this service, and what does it do?
The SLI for Database Freshness calculates how 'fresh' data for a mirror is by going through a set of tables and comparing
 it's last updated time with the time the service is run then averaging it out and appending a record into BigQuery.
 It also puts in a reference to a CSV uploaded to GCS with specific age details on the tables that were used in
 calculating the average.

## Who is responsible for it?
SRE team

## What dependencies does it have?
GCP services: Cloud Composer (Airflow), GCS, BQ, and GCR

## What does the infrastructure for it look like?
Single Python container aiming for low memory footprint with API calls to BQ and GCS.

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)
- [Freshness table on BQ](https://console.cloud.google.com/bigquery?sq=204727024514:c7db1cd510a640be9b1e5ee11bedce70)
- [Coverage table on BQ](https://console.cloud.google.com/bigquery?sq=204727024514:596a7d20fe3e4102beb2263646fe513d)

### Logs
- Container logs contain calls in the Python `logging` module from container

## What alerts are set up for it, and why?
When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.

## Tips from the authors and pros
- View logs on Airflow UI as a starting point for debugging any issues
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

## What to do in case of failure
- Manually trigger the DAG
- Alternatively (if that doesn't work) one can run the
 [Python code directly](../../containers/slo-tools/db_mirror/freshness_metric.py)
