# dbt Nightly
Running [dbt](https://www.getdbt.com/product/) models in BQ on a Nightly schedule 

## What is this service, and what does it do?
The dbt nightly job runs a set of models (translated to BQ queries) to update views
and tables that have the `nightly` tag. These are primarily abstractions built
on top of Commons Mirror which is why there is a hard dependency on it.
The Computed Fields Scio job is run after the dbt queries successfully execute as
the queries that make up that job depend on views/tables that have the `nightly` tag.

## Who is responsible for it?
Platform and Data Analytics team

## What dependencies does it have?
Internal Services: Commons Mirror, [panel management ingestion](https://github.com/cityblock/mixer/blob/master/cloud_composer/dags/load_panel_management_data_v4.py)

GCP services: Cloud Composer (Airflow), Cloud Build, GCR, Dataflow, and BigQuery

## What does the infrastructure for it look like?
TODO create system architecture on Lucidchart

Wait for Commons Mirror to complete export &#8594; dbt run &#8594; Computed Fields Scio job

All of this is orchestrated in the Airflow cluster as individual pods (exception of the wait).
The waiting for Commons Mirror is done via an [Airflow task sensor](https://airflow.apache.org/docs/stable/howto/operator/external.html) -
specifically to wait for the Commons Mirror job to complete.

The dbt job is executed on a container that contains the most recently built
dbt code (ideally have it execute git clone operations as that is far cleaner)

The Computed Fields job is based [entirely on an existing CronJob](https://github.com/cityblock/mixer/pull/1623)
that was migrated into Airflow

## What metrics and logs does it emit, and what do they mean?
### Metrics
- Airflow UI contains history of past runs and their states (success, fail, scheduled,
manual, etc.)
- dbt logs the model status for production runs in `cityblock-analytics.dbt.audit_dbt_results`

### Logs
- dbt run: python container, funneled into Airflow, and available on GKE pod view (for limited time)
- Scio job: Dataflow job in `cityblock-data` and Airflow

## What alerts are set up for it, and why?
- When any item in the DAG causes an exception, it is forwarded to the private
`airflow_alerts` Slack channel.
- There is a notification in the case of CF results being stale (over a day old) - this does not cause of a failure
 for the DAG. It sends an email to DA to look further into and debug why the data is stale.

## Tips from the authors and pros
- If the external task sensor dependency needs to be bypassed, trigger the DAG and
mark the external task sensor as "Success", and then the DAG will continue to the
downstream tasks

- If dbt tests fail, click the `Log:` link in the Airflow alert message to access the logs. From here, the output will be similar to any other dbt run. Identify the tests that have failed, and copy the compiled SQL from the relevant model pages on our [dbt Docs](https://data-docs.cityblock.com/dbt#!/overview) site. Use the compiled query to identify the source of the failure in BigQuery.

## Appendix
- [Cityblock dbt](../../dbt)
- [dbt container](../../dbt/cloud_build/prod/Dockerfile)
- [CF Scio job](../../scio-jobs/src/main/scala/cityblock/computedfields/jobs/NewProductionResults.scala)
