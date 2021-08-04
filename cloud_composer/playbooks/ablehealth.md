# Able Health Daily Ingestion
Ingests and processes Able Health Data into BigQuery and Quality Measure (QM) Service

## What is this service, and what does it do?
Able Health (AH) is a vendor that provides us with quality reporting data about our members. We provide AH
 data about our members in a batch job run once a day, and the next day we ingest the results of that into
 BigQuery and the QM Service.

## Who is responsible for it?
Platform team

## What dependencies does it have?
Internal Services: Quality Measure Service

GCP services: Cloud Composer (Airflow), GCS, and BigQuery

External Services: AH endpoint and AH S3 bucket

## What does the infrastructure for it look like?
[See diagram here for data flows](../../diagrams/data_flows/vendors/ablehealth.png)

All of this is orchestrated in the Airflow cluster as individual pods.

There are 2 distinct DAGs that handle both delivery and ingestion that run at specific times. These
 DAGs are independent in terms of their runtime, so if one fails the other one will run regardless.

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)

### Logs
- The Python application code contains lots of logging statements which can be inspected for further
 details on what is occurring in the pod.

## What alerts are set up for it, and why?
When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.

## Tips from the authors and pros
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

### Able Health Ingestion in BigQuery
#### Mismatch in schema
This occurs when the schema is changed by Able Health, you will see an exception message that looks like
 the following:
 ```
 pandas_gbq.gbq.InvalidSchema: Please verify that the structure and data types in the DataFrame match the schema of the destination table.
 ```
This means we need to update the schema on Terraform for the relevant table. In order to look at the new header
 use `gsutil` ([see `cat` reference here](https://cloud.google.com/storage/docs/gsutil/commands/cat)):
 ```
 gsutil cat -r 0-500 gs://us-east4-prod-airflow-d4e023bb-bucket/ablehealth_results/2021-01-02/risk_scores.csv
 ```
Compare this to a previous date schema to see what the difference is, once you've identified the change, implement
 it in the [Terraform code for the appropriate tables](../../terraform/cityblock-data/bigquery_schemas/ablehealth)
 
Please run this on your sandbox and ensure the updated version of the table runs successfully. 
 Use the console to copy the tables to your sandbox and update the schemas manually there.

## Appendix
- [Python application in mixer](../../containers/ablehealth)
- [Terraform references to table schemas](../../terraform/cityblock-data/bigquery_schemas/ablehealth)
