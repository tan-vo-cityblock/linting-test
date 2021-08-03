# QReviews to CBH DAG

## Table of Contents
- [Overview](#overview)
- [Debugging](#debugging)
	- [Task: `qreviews-to-bq`](#task-qreviews-to-bq)
	- [Task: `dbt_run`](#task-dbt_run)


## Overview

See [/containers/qreviews/README.md](../../containers/qreviews/README.md) for overview of the QReviews workflow.

## Debugging

### Task: `qreviews-to-bq`

__ERROR: QReviews updated / added fields in their json response:__

* The DAG task `qreviews-to-bq` is intended to fail if the qreviews response json schema is updated with new fields/columns that don't match 
our predefined BQ schema for the `cityblock-data.qreviews.qreviews_results` table.

__FIX__: 

* Communicate schema changes to the team and account for changes in downstream dependencies.

* Check the logs for BQ load errors to determine which field/column was added. The error message will usually indicate `no such field` at the json position.

* Edit the BQ [qreviews_results.json](../../terraform/cityblock-data/bigquery_schemas/qreviews/qreviews_results.json) schema file in terraform to add the new / offending column. Make sure the position of the column matches the json position from the previous bullet as certain fields can be inside of BQ records.

* Submit a PR with your changes and merge on approval.

* Apply your terraform changes, and re-trigger this dag.


### Task: `dbt_run`

Flag DS/DA team member to coordinate a fix.