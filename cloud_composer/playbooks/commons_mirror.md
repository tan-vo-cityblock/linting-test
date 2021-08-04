# Commons Mirror
Mirroring Commons PostgreSQL Database on a Twice-Daily Schedule

## What is this service, and what does it do?
The Commons Mirror job creates a mirror of our PostgreSQL database that is the primary
data store for our Commons application. It does this by first SSHing into our Aptible
instance, provisioning a backup (we don't want to run operations on prod even if in the
middle of the night), exporting schema and data files as CSVs and copying them to a GCS
bucket. Afterwards we run a series of Dataflow jobs that take those files from GCS and
produces tables on BigQuery in a shard based on the date the job is run on. Finally we do
some 'light transformations' which put it back into that same dataset.
At this point the data should be ready for downstream consumption.

## Who is responsible for it?
Platform team

## What dependencies does it have?
Internal Services: Commons

GCP services: Cloud Composer (Airflow), Cloud Build, GCR, GCS, Dataflow, KMS, IAM,
and BigQuery

Aptible services: Deploy

## What does the infrastructure for it look like?
[See diagram here](../../diagrams/data_flows/internal/db_mirroring.png)

All of this is orchestrated in the Airflow cluster as individual pods.

The Aptible operations occurs in the Aptible instance including a
streaming the files into GCS (not stored on disk). The GCS bucket has a lifecycle
rule of deleting all data within 2 days.
<br>
There is a hard export for the Builder tables in this process that calls a Node
job to do this.

The Scio job executes in "chunks" on Dataflow. A chunk is a list of tables/schemas.

The Transformers are containers (currently NodeJS) that takes previous output and feeds back into
 the same dataset.

The Updating of the BQ views occurs via API calls to BQ service.

Coverage SLI Metric is a Python process that makes API calls to BQ service.

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)
- The `cbh-db-mirror-prod.commons_mirror.commons_mirror_errors_<SHARD_NAME>` table
on BQ contains rows that failed to be parsed in the Scio jobs
- The SLI coverage metric uploads a summary of the data coverage based on row counts of 
tables output from the Scio job and the errors table

### Logs
- Aptible export: echo statements throughout container application, funneled into
Airflow, and available on GKE pod view (for limited time)
- Scio jobs: Dataflow jobs in `cbh-db-mirror-prod`
- Update BQ views: statements throughout container application, funneled into
Airflow, and available on GKE pod view (for limited time)
- Coverage SLI: statements throughout container application, funneled into
Airflow, and available on GKE pod view (for limited time)

## What alerts are set up for it, and why?
When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.

## Tips from the authors and pros
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

### Updating views failed
#### A table name ending with `_transformed_DATE`
Look at the associated upstream task on the DAG that has the same table name, inspect those logs
 to see if that job had failed 'softly' (usually through `UnhandledPromiseRejectionWarning`).
 
You can retry this task to see if it succeeds, if so you can clear and retry the downstream tasks
 to ensure everything else runs as intended
 
If retrying is unsuccessful, then you must debug further as to why the upstream task failed,
 [consult the container code to debug locally](../../containers/draftjs-transformer)

## Appendix
- [Aptible export container](../../containers/commons-to-bq-mirroring)
- [Scio package](../../scio-jobs/src/main/scala/cityblock/importers/mirroring)
- [Draft.js Transformer](../../containers/draftjs-transformer)
- [Updating BQ views script](../../containers/cloud-sql-to-bq-mirroring/update_bq_views.py)
- [Design doc for migrating away from Node Script (original Commons Mirror job)](
https://docs.google.com/document/d/1NeCBdNt9oj67IRmvqlTJYANSYT1VLPuW7KJWp8B0UiM/edit)
