
# Elation Mirror DAG

## Table of Contents
- [What is this service, and what does it do?](#what-is-this-service-and-what-does-it-do)
- [Who is responsible for it?](#who-is-responsible-for-it)
- [What dependencies does it have?](#what-dependencies-does-it-have)
  - [Schedule Dependency](#schedule-dependency)
  - [Commons External Dependency to DAG task push_elation_patient_encounters](#commons-external-dependency-to-dag-task-push_elation_patient_encounters)
- [What does the infrastructure for it look like?](#what-does-the-infrastructure-for-it-look-like)
  - [Task - `elation_db_to_gcs_and_bq`](#task---elation_db_to_gcs_and_bq)
  - [Task - `push_elation_patient_encounters_job`](#task---push_elation_patient_encounters_job)
  - [Tasks - `dbt_run_elation` >> `dbt_test_elation`](#tasks---dbt_run_elation--dbt_test_elation)
  - [Tasks - `update_ae_firewall_if_need_be` >> `elation_to_qm_svc`](#tasks---update_ae_firewall_if_need_be--elation_to_qm_svc)
- [What metrics and logs does it emit, and what do they mean?](#what-metrics-and-logs-does-it-emit-and-what-do-they-mean)
- [What alerts are set up for it, and why?](#what-alerts-are-set-up-for-it-and-why)
- [Tips from the authors and pros](#tips-from-the-authors-and-pros)
  - [Debugging `elation_db_to_gcs_and_bq`](#debugging-elation_db_to_gcs_and_bq)
  - [SSH into Elation Server to access MySQL DB:](#ssh-into-elation-server-to-access-mysql-db)

## What is this service, and what does it do?

[Elation](https://www.elationhealth.com/) is our clinical Electronic Health Records (EHR) platform of choice in all virtual markets and non-virtual markets other than NYC. Elation provides access to a hosted MySQL database that holds data from our EHR instance(s).
Request access to [Elation's Hosted DB Reference google sheet](https://docs.google.com/document/d/1ukLrkikXHx727h4nE2mSmxf8X73Qisbh-q_gC2tbgn8/edit) 
to learn more about the tables and fields contained within.

Elation Mirror DAG code is defined in [/dags/mirror_prod_elation_v[n].py](../dags/).

## Who is responsible for it?
Platform team.


## What dependencies does it have?


#### Schedule Dependency

The Elation Mirror DAG mirrors our Elation Hosted MySQL DB from Elation's SSH server into GCS and BigQuery. 
Elation refreshes the hosted DB daily at 04:00AM UTC / 12:00AM EDT and the refresh completes by ~ 04:30AM UTC / 12:30AM EDT.

The DAG kicks off daily at 05:00AM UTC / 01:00AM EDT after the Elation refresh is expected to complete. The DAG completes
the export and mirroring in ~25 minutes. In the future, Elation has indicated they plan to refresh the hosted DB multiple 
times a day; in that case we will update the DAG to kick-off mirroring after each refresh.

#### Commons External Dependency to DAG task push_elation_patient_encounters and DAG batch_refresh_clinical_summaries_v2

**TODO: UPDATE WHEN [PLAT-1714](https://jira.cityblock.com/browse/PLAT-1714) IS DONE.**

The `run_commons_cache_ccd_encounter` DAG task runs to consume the output of the `push_elation_patient_encounters` DAG task
and the `batch-update-redox-clinical-summaries` task in DAG `batch_refresh_clinical_summaries_v[x]`.

The `run_commons_cache_ccd_encounter` runs code in [commons/server/jobs/cache-ccd-encouters](https://github.com/cityblock/commons/blob/master/server/jobs/cache-ccd-encounters.ts) that will ultimately lead to the Commons Timeline reflecting up-to-date notes from EHRs.

**If any upstream tasks fail thereby preventing the Commons' task from running or consuming complete data, it is important that we resolve them before the start of the workday (~8AM) and re-run the task. If we can't resolve in time, we should notify folks in slack channels #elation-feedback and #analytics-support that EHR data will be stale until resolved.**

You can monitor failures of the dependent Commons task in Airflow logs and in GCP project 
[commons-production Error Reporting](https://console.cloud.google.com/errors?time=P7D&filter=text:cacheCcdEncounter&order=LAST_SEEN_DESC&resolution=OPEN&resolution=ACKNOWLEDGED&project=commons-production) by querying `cacheCcdEncounter`. Failures here may require updates to the task or batch job logic.

Once upstream failures in this DAG are resovled, manually clear the failing task in the airflow UI to re-trigger the Commons' task.

If the Commons' task needs to be run manually due to a failure in the external DAG `batch_refresh_clinical_summaries_v[x]`, grab a Prod Engineer for help doing this in Commons. 

The high level steps are:

1. [Back up the prod Commons database](https://github.com/cityblock/commons/tree/6551b425f0c7ba8bf55eff31c0ccfe67584101c7#back-up-commons-database)
2. [Open the Kue Dashboard to monitor jobs that will be added](https://github.com/cityblock/commons/tree/6551b425f0c7ba8bf55eff31c0ccfe67584101c7#view-status-of-background-jobs)
3. [SSH into Commons prod app to run the job](https://github.com/cityblock/commons/tree/6551b425f0c7ba8bf55eff31c0ccfe67584101c7#ssh-into-commons)
4. [Tunnel into Commons prod database to monitor rows are being added to `ccd_encounter_cache` table](https://github.com/cityblock/commons/tree/6551b425f0c7ba8bf55eff31c0ccfe67584101c7#to-connect-to-the-aptible-database-from-your-machine-run)
5. Inside the Aptible app, run `npm run jobs:cache-ccd-encounters:production` wait until you see an exit code. Exit code may indicate a fluke error even when successfully completed.

## What does the infrastructure for it look like?


#### Task - `elation_db_to_gcs_and_bq`

The task is carried out by a `KubernetesPodOperator` and uses the python image code defined in [elation_mysql_mirror.py](../../containers/cloud-sql-to-bq-mirroring/elation_mysql_mirror.py) to export tables as csv files from Elation, store the csv files in 
[GCS namespaced by export date](https://console.cloud.google.com/storage/browser/cbh-export-elation-prod/elation_db?project=cbh-db-mirror-prod&prefix=) 
, and then load the csv files into BigQuery tables in the [cbh-db-mirror-prod.elation_mirror dataset](https://console.cloud.google.com/bigquery?project=cbh-db-mirror-prod&p=cbh-db-mirror-prod&d=elation_mirror&page=dataset).


#### Task - `push_elation_patient_encounters_job`

The task is carried out by a `KubernetesPodOperator` that uses our `mixer_scio_jobs` image to run
the scio batch job `PushElationPatientEncounters` in Dataflow.

See the [README](../../scio-jobs/README.md#pushelationpatientencounters) for the batch job in scio for more on it's infra and outputs.

#### Tasks - `dbt_run_elation` >> `dbt_test_elation`

The tasks are carried out by `KubernetesPodOperators` to run and test dbt models that query Elation mirror tables as sources.

#### Tasks - `update_ae_firewall_if_need_be` >> `elation_to_qm_svc`

The tasks are carried out by `KubernetesPodOperators` to first run a cloud function to whitelist the Airflow IPs in app engine (`update_ae_firewall_if_need_be`). See the cloud function [README](../../cloud_functions/airflow_ae_firewall_allow/README.md) for more.

After successful whitelisting, the `elation_to_qm_svc` task kick-offs the python image defined in [elation_to_qm_service.py](../../containers/elation-utils/elation_mysql_mirror.py) to post to the QM Service in app engine. See the QM service's [README](../../services/quality_measure/README.md#elation) for more.


## What metrics and logs does it emit, and what do they mean?

- All tasks emit logs in Airflow
- `elation_db_to_gcs_and_bq`: Emits logs to [cbh-db-mirror-prod.elation_mirror.MIRROR_ERRORS](https://console.cloud.google.com/bigquery?project=cbh-db-mirror-prod&p=cbh-db-mirror-prod&d=elation_mirror&t=MIRROR_ERRORS_latest&page=table) and [cbh-db-mirror-prod.elation_mirror.MIRROR_METADATA](https://console.cloud.google.com/bigquery?project=cbh-db-mirror-prod&p=cbh-db-mirror-prod&d=elation_mirror&t=MIRROR_METADATA_latest&page=table)
- `push_elation_patient_encounters_job`: Emits to [Dataflow](https://console.cloud.google.com/dataflow/jobs?project=cityblock-data) in `cityblock-data`.
- `airflow_ae_firewall_allow`: Emits logs to [Cloud Functions](https://console.cloud.google.com/functions/details/us-east4/airflow_ae_firewall_allow?project=cityblock-data&tab=general) in `cityblock-data`.
- `elation_to_qm_svc`: Emits logs in [App Engine](https://console.cloud.google.com/appengine?project=cbh-services-prod&serviceId=quality-measure) and [CloudSQL](https://console.cloud.google.com/sql/instances/services/overview?project=cbh-services-prod) in `cbh-services-prod`.

## What alerts are set up for it, and why?
- When any task in the DAG causes an exception, it is forwarded to the private
`#airflow_alerts` Slack channel.
- TODO: SETUP MORE ALERTS SPECIFIC TO EACH TASK


## Tips from the authors and pros

#### Debugging `elation_db_to_gcs_and_bq`

__ISSUE__: Elation failed to refresh the hosted db on schedule
   
   Elation should refresh the hosted db at least once daily per the schedule above. Sometimes, due to maintenance or 
   issues on their end, they may fail to refresh the hosted db.
   
__FIX__:

* Reach out to someone on the product team who manages our integration/comms with Elation about the stale DB.

* If Elation performs an ad hoc refresh of the DB outside of the pre-defined schedule, manually trigger the DAG  accordingly.


__ISSUE__: BQ Load job failing on bad rows in the csv.


__FIX__: 

- Depending on the type of issue and number of rows impacted, you can temporarily 
quiet the load errors by increasing the number of allowed errors for a table's load job to > 0. Note that BQ's [--max_bad_records](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference) returns a max of 5 errors per type.
   - You can do so by editing the ElationError class const `BAD_RECORDS_ALLOWED` dictionary in [elation_mirror_helper.py](../../containers/cloud-sql-to-bq-mirroring/elation_mirror_helper.py):
   ```
   BAD_RECORDS_ALLOWED = {
        'table_name': 1
    }
   ```

- Check the `elation_mirror.MIRROR_ERRORS` to determine the location in the CSV.
- Find and download the csv file in bucket `cbh-export-elation-prod` to inspect it.
- If it's unclear, ssh into the Elation server to inspect the row in the MySQL table directly to understand why parsing might be failing. See SSH instructions below.
- If a code fix feels complicated or overly specific, don't be shy about contacting Elation to see if they can fix it on their end.
   
__ISSUE__: Elation updated a table's schema in a breaking way.

The Elation mirror should be able to handle schema changes according to [BQ's supported schema modifications](https://cloud.google.com/bigquery/docs/managing-table-schemas) outlined in the docs.

__FIX__: For unsupported schema changes per the BQ docs:

These changes should be rare, so alert Kola, before continuing as it may require contacting Elation to better understand the reason for the change.

The general approach to a fix will involve:

- Copying all previous partitions of the table to a new set of partitions namespaced: `elation_mirror.[table]_depr_1` where `depr` means deprecated, and `1` will represent a monotomically increasing version number for deprecated tables.
- Delete the original table partitions that was copied in the previous step. This will also clear the schema for table.
- Re-trigger the task which will automatically re-create the table with the updated schema and subsequent `latest` view.


For reference, the below table represents how MySQL types are mapped to BQ types
in the `ElationMetadata` class defined [elation_mirror_helper.py](../../containers/cloud-sql-to-bq-mirroring/elation_mirror_helper.py):

| MySQL Types                               | BigQuery Types |
   |:-----------------------------------------:|:--------------:|
   | TINYINT(>1), SMALLINT, INT, BIGINT        | INTEGER        | 
   | DECIMAL, FLOAT, DOUBLE                    | FLOAT          |
   | CHAR, VARCHAR, TEXT, MEDIUMTEXT, LONGTEXT | STRING         |
   | TINYINT(1)                                | BOOLEAN        |
   | DATETIME                                  | DATETIME       |
   | Null -> NO                                | REQUIRED       |
   | Null -> YES                               | NULLABLE       |


#### SSH into Elation Server to access MySQL DB:

TODO: UPDATE WITH INSTRUCTIONS TO GET SECRETS FROM `secret_manager`.
* [Pull down and decrypt](../../terraform/README.md#cli---updating-encrypted-secrets) Elation secrets using key ring
      `cbh-elation-prod` and keys `ssh-config`, `ssh-private-key`, `known-host`, and `mysql-config` into a local folder 
      called `elation-secrets`. 
* Note the instructions for local secret file-naming conventions in the provided link.
* From your terminal, cd into the directory where you created the `elation-secrets` directory.
* Update file permissions for the secrets: `chmod 600 ./*`

* Run the below command to copy mysql creds into the Elation server and then ssh:
   
   ```bash
   ssh -F elation-secrets/ssh-config elation -t "rm -rf cbh-temp && mkdir -m 700 cbh-temp" \
   && scp -F elation-secrets/ssh-config elation-secrets/my.cnf elation:cbh-temp/ \
   && ssh -F elation-secrets/ssh-config elation
   ```
* Once inside the Elation server, run the below command to start the MySQL shell.
   ```bash
   mysql --defaults-extra-file=cbh-temp/my.cnf
   ```
* After running your queries, enter `exit` to exit the MySQL shell.

* Run the below to clean up and exit the ssh server:
  ```bash
  rm -rf cbh-temp && exit   
  ```
* Delete locally stored secrets: `rm -rf elation-secrets`
