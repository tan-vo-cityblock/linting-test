# SFTP Backup
Backing up SFTP data on GCS bucket into [cold storage](https://cloud.google.com/storage/docs/storage-classes#coldline)

## What is this service, and what does it do?
SFTP Backup takes a copy of the SFTP GCS bucket and put it into a cold storage
bucket. It is run once a day to ensure the files are up to date via a 
[`rsync` operation.](https://linux.die.net/man/1/rsync)

## Who is responsible for it?
Platform team

## What dependencies does it have?
Internal Services: SFTP GKE workload

GCP services: Cloud Composer (Airflow) and GCS

## What does the infrastructure for it look like?
Very simple architecture - essentially a cron schedule for a `gsutil` command.
All the processing work will be mitigated to GCS internals (copying data and parallelizing run)

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)

### Logs
- `gsutil rsync` logs containing the data copied

## What alerts are set up for it, and why?
When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.

## Tips from the authors and pros
- View logs on Airflow UI as a starting point for debugging any issues
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

## What to do in case of failure
- Manually trigger the DAG to force a resync (fresh backup)
- Alternatively (if that doesn't work) one can run the `gsutil` command from their local machine

## Appendix
- [SFTP application](../../sftp/README.md)
- [`gsutil rsync` reference](https://cloud.google.com/storage/docs/gsutil/commands/rsync)
