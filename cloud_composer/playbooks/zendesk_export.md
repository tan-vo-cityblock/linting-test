# Zendesk Bulk and Incremental Export

## What is this service, and what does it do?

See the [Workflow](../../containers/zendesk/README.md#workflow) section in the `containers/zendesk/README` for an
understanding of the pipeline.

See the docstrings at the top of each dag file (`dags/[env]_zendesk_[bulk/incremental]_export_v[x].py`) for
an understanding of the DAG schedule and tasks.

## Who is responsible for it?
Platform team.

## What dependencies does it have?

### Code Dependencies

The container code depends on the [Zendesk Support API](https://developer.zendesk.com/rest_api/docs/support/introduction),
and the [Zenpy Python Wrapper](https://github.com/facetoe/zenpy) which is a handy abstraction over the Zendesk API.

## What does the infrastructure for it look like?

The container code is hosted in our [Container Registry](https://console.cloud.google.com/gcr/images/cityblock-data/US/zendesk?project=cityblock-data&gcrImageListsize=30)
via Docker builds in [containers/cloudbuild.yaml](../../containers/cloudbuild.yaml).

See the [Outputs](../../containers/zendesk/README.md#outputs) section in the `containers/zendesk/README` for links
to infrastructure that hold the outputs of this DAG.

## What metrics and logs does it emit, and what do they mean?

See the [metadata and log](../../containers/zendesk/README.md#outputs) sections in the `containers/zendesk/README` for details
on the metrics and logs emitted.

## What alerts are set up for it, and why?

- When any task in the DAG causes an exception, it is forwarded to the private
[airflow_alerts](https://cityblockhealth.slack.com/archives/GJD71N10S) Slack channel.

## Tips from the authors and pros

- Read the [containers/zendesk/README](../../containers/zendesk/README.md#outputs).

## Debugging

A common, though rarely occurring error, can happen due to a mismatch between the Zendesk API JSON response and the
established BQ schema where the Zendesk model is written to.

The [zendesk export code](../../containers/zendesk/exporter/base_exporter.py) automatically handles adding new columns to the schema
definition and relaxing a column's mode from `REQUIRED` to `NULLABLE`.

All other schema changes require manual workarounds. See below and [BQ docs](https://cloud.google.com/bigquery/docs/managing-table-schemas) for more.

### Previously all null column (default type = STRING) is populated with a non-STRING value type

An existing column that had no values and therefore defaulted to NULL (type = String) is now populated with a non-STRING value causing schema to break.

**DDL fix in BQ UI**:

- Create a new temp table in your personal project (include `temp` in name to avoid confusion) by copying all the columns except the affected column,
  and re-create the affected column with it's correct type.
- Example query that would be [saved to a new temp table via UI](https://cloud.google.com/bigquery/docs/writing-results#writing_query_results):
   ```
   SELECT
     * EXCEPT(target_col),
     CAST(target_col AS INT64) AS target_col
   FROM
     `project.dataset.table`
   ```

- After creating this temp table, delete the original table, and copy the temp table back to the original table's name/destination.
- When the job runs again, the new table will have a compatible updated schema.
- Finally, once the fix is confirmed to work, delete the temp table you created.

### Column renamed, column deleted or complex type (STRUCT / ARRAY) changes

- When a column is renamed, deleted, or a struct / nested array is changed in ways that make the above fix overly complex, it is safe
  to consider this a breaking change in the Zendesk API model.

- This instability in the API can happen because Zendesk allows our Cityblock admins to create custom fields in various Zendesk models.
- Rather than trying to transform previous export data to fit the breaking changes, we can simply mark the old table as deprecated and create a new one that will
   automatically accommodate the changes.
- Another reason for deprecating rather than transforming is that we want to preserve previous export data so that they are stateful and accurately
  represent data from that point in time.

**Deprecate table in BQ for breaking change:**
- [Copy old table to new table in same dataset](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) and append `_depr_[n]` to the name where [n] is a monotonically increasing integer starting at 1.

- Confirm copy is done correctly, (i.e. for partitioned tables, confirm new copy retains partitioning).
- Delete old table, and do nothing.
- On next job run, the table will be automatically recreated to accommodate the breaking schema changes.
- Confirm table is automatically re-created after next job run.
- See table [cityblock-data.zendesk.suspended_ticket_depr_1](https://console.cloud.google.com/bigquery?project=cityblock-data&d=zendesk&p=cityblock-data&t=suspended_ticket_depr_1&page=table)

