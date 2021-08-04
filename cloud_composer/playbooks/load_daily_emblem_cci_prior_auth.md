# Load Daily Prior Auth Data (Emblem & CCI)
Ingests and processes Prior Authorization data into BigQuery

## What is this service, and what does it do?
Our partner provides a Prior authorization file into the SFTP drive for each of our partners every day
 and this service is responsible for taking it and propagating it into BigQuery. It starts by taking the
 file itself and running it through the `LoadToSilverRunner`, after that it updates the view associated with
 this table and finally does the dbt transformation.

## Who is responsible for it?
Platform team

## What dependencies does it have?
GCP services: Cloud Composer (Airflow), GCS, Dataflow, IAM, and BigQuery

## What does the infrastructure for it look like?
[See diagram here for Emblem](../../diagrams/data_flows/partners/emblem_ingestion.png)
<br>
[See diagram here for CCI](../../diagrams/data_flows/partners/cci_ingestion.png)

All of this is orchestrated in the Airflow cluster as individual pods.

The partner steps (`LoadToSilver` Scio job and `update_views` Python script) are done
 independently. They converge on the dbt step which runs the relevant transforms altogether.

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)

### Logs
- Scio jobs: Dataflow jobs in `cityblock-data`
- Update BQ views: statements throughout container application, funneled into
Airflow, and available on GKE pod view (for limited time)

## What alerts are set up for it, and why?
When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.

## Tips from the authors and pros
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

### How the Prior Auth file is handled
#### No file on GCS with prefix
This most likely means the file is not yet available at the time of running the job. Navigate to the
 SFTP drop GCS bucket to check if the file is available, this can be done on the command line in the
 following example:
 ```
PREFIX=emblem/drop/CORP_CITYBLOCK_MULTI_PAUNIRPT_CAE_F_D_PROD_20201130
gsutil ls gs://cbh_sftp_drop/$PREFIX*
```
It will output the file if it exists (ensure there is only one, otherwise when running again you will get
 the following error). If not yet available you should get: `CommandException: One or more URLs matched no objects.`

Once the file is available, it is safe to clear the task and let the Airflow scheduler retry. If there is a
 significant delay for this data, reach out the Product Manager for guidance.

#### Multiple files matched for prefix
This means there were multiple uploads for the Prior Auth data by our partner, confirm this by either navigating to the 
 GCS bucket or running the command as follows:
 ```
PREFIX=emblem/drop/CORP_CITYBLOCK_MULTI_PAUNIRPT_CAE_F_D_PROD_20201124
gsutil ls -lh gs://cbh_sftp_drop/$PREFIX*

// example output
135.94 MiB  2020-11-24T09:21:59Z  gs://cbh_sftp_drop/emblem/drop/CORP_CITYBLOCK_MULTI_PAUNIRPT_CAE_F_D_PROD_20201124030920.txt
136.24 MiB  2020-11-24T10:21:59Z  gs://cbh_sftp_drop/emblem/drop/CORP_CITYBLOCK_MULTI_PAUNIRPT_CAE_F_D_PROD_20201124031020.txt
```
It should output multiple files. In order for this job to work we need only one file. Chances are in this output
 one of the files will be more recent than the others and also contain more data (confirm with the size). [Delete all
 the other files in the SFTP drive](../../sftp/README.md#getting-in---only-in-exceptional-cases) and ensure only the 
 one we care about is present. If in doubt, reach out to the Product Manager about this. Once the file(s) is deleted in 
 the SFTP drive, ensure it is also removed from the GCS bucket. 
At this point, it is safe to clear the task and let the Airflow scheduler retry.

## Appendix
- [`LoadToSilverRunner`](../../scio-jobs/README.md#loadtosilver)
- [Updating BQ views script](../../containers/load_monthly_data/update_views.py)
