# Zendesk -> Mixer

[Zendesk](https://www.zendesk.com/support/) is a task management tool that helps businesses support their customers.

Our Cityblock Virtual Hub Care Teams are tracking and managing their care work in Zendesk.

This `containers/zendesk` directory holds image code to export data from our Zendesk instance into GCP via
the [Zendesk Support API](https://developer.zendesk.com/rest_api/docs/support/introduction).

## Table of Contents

* [Environments](#environments)
* [Outputs](#outputs)
    * [Export Data - GCS to BQ](#export-data---gcs-to-bq)
    * [Export Metadata Logs - BQ](#export-metadata-logs---bq)
    * [Ticket Messages - PubSub](#ticket-messages---pubsub)
    * [Ticket Message Error Logs - BQ](#ticket-message-error-logs---bq)
* [Workflow](#workflow)
    * [Export](#export)
        * [Bulk Export](#bulk-export)
        * [Incremental Export](#incremental-export)
    * [Transformed Ticket to PubSub](#transformed-ticket-to-pubsub)
        * [Unpublished tickets due to user join errors](#unpublished-tickets-due-to-user-join-errors)
* [Setup](#setup)
    * [Pull Secrets](#pull-secrets)
    * [Running via Docker](#running-via-docker)
        * [Docker exec run bulk export](#docker-exec-run-bulk-export)
        * [Docker exec run incremental export](#docker-exec-run-incremental-export)
        * [Docker exec publish tickets](#docker-exec-publish-tickets)
        * [Docker exec replay published tickets](#docker-exec-replay-published-tickets)
        * [Docker clean up](#docker-clean-up)
    * [Development](#development)

## Environments

We have a Zendesk prod instance available via OneLogin and a [sandbox/staging instance](https://cityblock1587564213.zendesk.com/)
available directly by visiting the link.

To gain access to the staging instance, post your request in the [#platform-support](https://cityblockhealth.slack.com/archives/CAQNY165D)
channel and an engineer with admin access will be able to add / invite you.

Each Zendesk instance has it's own credentials. See below section [Pull Secrets](#pull-secrets) for more on accessing the credentials.

## Outputs

### Export Data - GCS to BQ

GCS bucket stores exported json files (life cycle of 2 days) that are uploaded to BigQuery:
- [gs://zendesk_export_prod/{model}/{timestamp}/{model}.json](https://console.cloud.google.com/storage/browser/zendesk_export_prod;tab=objects?project=cityblock-data&prefix=&forceOnObjectsSortingFiltering=false)
- [gs://zendesk_export_staging/{model}/{timestamp}/{model}.json](https://console.cloud.google.com/storage/browser/zendesk_export_staging;tab=objects?forceOnBucketsSortingFiltering=false&project=staging-cityblock-data&prefix=&forceOnObjectsSortingFiltering=false)

BQ dataset holding exported models:
- [cityblock-data.zendesk](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&page=dataset)
- [staging-cityblock-data.zendesk](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=zendesk&page=dataset)
- **For the latest, complete state of data, reference the BQ view with `latest` in name, i.e. `zendesk.model_table_latest`.**

### Export Metadata Logs - BQ

BQ table storing metadata related to each model/table export, i.e. cursor values for incrementally exported values:
- [cityblock-data.zendesk.EXPORT_METADATA](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&t=EXPORT_METADATA&page=table)
- [staging-cityblock-data.zendesk.EXPORT_METADATA](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=zendesk&t=EXPORT_METADATA&page=table)


### Ticket Messages - PubSub

BQ table storing data on Zendesk tickets with user fields that were not joined to internal Cityblock member / user ids:
- [cityblock-data.zendesk.TICKET_USER_JOIN_ERROR](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&t=TICKET_USER_JOIN_ERROR&page=table)
- [staging-cityblock-data.zendesk.TICKET_USER_JOIN_ERROR](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=zendesk&t=TICKET_USER_JOIN_ERROR&page=table)
- Table created to debug join issues and provide visibility into tickets that were not published due to user join issues.

PubSub topic for publishing transformed Zendesk Ticket messages:
- [zendeskTicket](https://console.cloud.google.com/cloudpubsub/topic/detail/zendeskTicket?project=cityblock-data)
- [stagingZendeskTicket](https://console.cloud.google.com/cloudpubsub/topic/detail/stagingZendeskTicket?project=cityblock-data)
- [devZendeskTicket](https://console.cloud.google.com/cloudpubsub/topic/detail/devZendeskTicket?project=cityblock-data)

PubSub subscriber that Commons uses to subscribe to ticket messages:
- [commonsZendeskTicket](https://console.cloud.google.com/cloudpubsub/subscription/detail/commonsZendeskTicket?project=cityblock-data)
- [stagingCommonsZendeskTicket](https://console.cloud.google.com/cloudpubsub/subscription/detail/stagingCommonsZendeskTicket?project=cityblock-data)
- [devCommonsZendeskTicket](https://console.cloud.google.com/cloudpubsub/subscription/detail/devCommonsZendeskTicket?project=cityblock-data)
- Subscription logic lives in commons repo [server/consumers/zendesk-ticket-consumer](https://github.com/cityblock/commons/blob/master/server/consumers/zendesk-ticket-consumer.ts).


### Ticket Message Error Logs - BQ

BQ table storing ticket messages published to PubSub. Should be used to replay messages as needed.
Also read in [publish_tickets.py](./publish_tickets.py) to generate ticket messages diff and send only new messages:
- [cityblock-data.pubsub_messages.zendesk_ticket](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=pubsub_messages&t=zendesk_ticket&page=table)
- [staging-cityblock-data.pubsub_messages.zendesk_ticket](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=pubsub_messages&t=zendesk_ticket&page=table)

## Workflow

This image has 3 main workflows
1. Exporting of Zendesk data models via bulk API endpoints
1. Exporting of data models via incremental API endpoints
1. Publishing of transformed Zendesk Tickets to PubSub.

The exporting of models is facilitated by the [Zenpy Python Wrapper](https://github.com/facetoe/zenpy) around the Zendesk API.

We've customized the Zenpy Client a bit in [./export_client/custom_zenpy.py](./export_client/custom_zenpy.py)
to add some features for our use case, specifically, around incremental exports.

### Export

Both incrementally and bulk exported models are saved in BQ dataset
[zendesk](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&page=dataset)
and along with
[metadata](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&t=EXPORT_METADATA&page=table)
regarding each export.

- An example query to get an idea of the history of cursor based incremental exports would be:
  ```
  SELECT * FROM `cityblock-data.zendesk.EXPORT_METADATA`
  WHERE table_name = 'ticket' ORDER BY bq_inserted_at DESC
  ```

#### Bulk Export

For most Zendesk models, Zendesk provides bulk endpoints that allow us to grab all data
available each time we hit the endpoint.

The code logic for bulk exporting lives in [./exporter/base_exporter.py](./exporter/base_exporter.py).

Bulk exported models live in day partitioned BQ tables with the latest state materialized in a view
pointing to the latest partition.

The views have suffix `latest`, i.e. `zendesk.model_table_latest`, and should be used when working on the latest complete state of data.

#### Incremental Export

Zendesk provides [incremental endpoints](https://developer.zendesk.com/rest_api/docs/support/incremental_export)
for a select list of models that allow us to export data using cursors from a provided point in time.

This is done because these models are constantly created / updated, so bulk exporting them would
be inefficient.

The code logic for incrementally exporting lives in [./exporter/incremental_exporter.py](./exporter/incremental_exporter.py).

Incrementally exported models live in standard BQ tables that are appended after each cursor based incremental export.

The append only table can have duplicates due to the nature of incremental exports, so the latest state is also materialized
in a view with deduplication logic.

The views have suffix `latest`, i.e. `zendesk.model_table_latest`, and should be used when working on the latest complete state of data.

### Transformed Ticket to PubSub

[Zendesk Ticket](https://developer.zendesk.com/rest_api/docs/support/tickets) is one of
the models that are incrementally exported.

After exporting Zendesk tickets, the code in [./publisher/ticket_publisher.py](./publisher/ticket_publisher.py)
first performs transformations on the tickets to join Zendesk users to internal Cityblock users and members.

Then, the transformed tickets are posted to PubSub topic
[zendeskTicket](https://console.cloud.google.com/cloudpubsub/topic/list?project=cityblock-data)
(suffixed by environment) to be consumed by Commons.

Published tickets are saved to [BQ](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=pubsub_messages&t=zendesk_ticket&page=table) for replay / audit purposes.

#### Unpublished tickets due to user join errors
Tickets that were not published due to errors joining user fields go to the [TICKET_USER_JOIN_ERROR table](https://console.cloud.google.com/bigquery?project=cityblock-data&p=cityblock-data&d=zendesk&t=TICKET_USER_JOIN_ERROR&page=table).
Once user join issues are resolved via the Member Service, these tickets will automatically be published on the
next run.

- Query to see latest state of unpublished tickets due to error:
  ```
  SELECT * FROM `cityblock-data.zendesk.TICKET_USER_JOIN_ERROR` AS unpublished
  LEFT JOIN `cityblock-data.pubsub_messages.zendesk_ticket` AS published
  ON unpublished.ticket_id = CAST(JSON_EXTRACT_SCALAR(published.attributes, '$.ticket_id') AS INT64)
  WHERE published.attributes IS NULL
  ```

## Setup

**Note: all commands below use the staging environment. To run in prod, update commands
by changing `staging` to `prod` except in the case of `staging-cityblock-data` which
changes to `cityblock-data`.**

### Pull Secrets
- See the [Secret Manager README](../../terraform/README.md#secret-management) for more details on
  how we manage secrets.

- Pull down Zendesk credentials and store locally by running the below:

  ```
  mkdir -p ~/zendesk-secrets
  bash ~/mixer/scripts/secret_manager/pull_secret.sh \
  staging-zendesk-api-creds \
  ~/zendesk-secrets/staging-zendesk-api-creds.json
  ```

- Pull down the zendesk worker service account key and store locally:

  ```
  bash ~/mixer/scripts/secret_manager/pull_secret.sh \
  staging-zendesk-worker-key \
  ~/zendesk-secrets/staging-zendesk-worker-key.json
  ```

### Running via Docker

In production, the `containers/zendesk` image is dockerized and run in gcloud GKE pods.

You can run the code locally / manually using Docker without having to go through
the process of setting up a local python environment.

- Download [Docker Desktop](https://www.docker.com/products/docker-desktop)

- To pull the image from our Container Registry, run:

    `docker pull us.gcr.io/cityblock-data/zendesk:latest`

- [optional] if you want to build the image from your local repo, run the above then run:

   ```
   docker build -t us.gcr.io/cityblock-data/zendesk:[your_tag] \
   --cache-from us.gcr.io/cityblock-data/zendesk:latest .
   ```

- Start the zendesk container image, bindmount your local `~/zendesk-secrets/` directory
  so they are available in the container, and set environment variables expected by
  the python code.
   ```
   docker run --rm -itd \
   --name zendesk \
   -v ~/zendesk-secrets/:/zendesk-secrets \
   -e GOOGLE_APPLICATION_CREDENTIALS='/zendesk-secrets/staging-zendesk-worker-key.json' \
   -e ZENDESK_SECRET_PATH='/zendesk-secrets/staging-zendesk-api-creds.json' \
   us.gcr.io/cityblock-data/zendesk:latest
   ```

#### Docker exec run bulk export

- To bulk export all models defined in the dictionary `BULK_GENERATOR_HELPERS`
  in file [exporter/bulk_exporter.py](./exporter/bulk_exporter.py):

   ```
   docker exec -it zendesk \
   python run_export.py \
   --project=staging-cityblock-data --dataset=zendesk \
   --bucket=zendesk_export_staging --bulk_export
   ```

- [optional] You can selectively bulk export a subset of models defined in the dictionary `BULK_GENERATOR_HELPERS`
  by appending this flag and space delimited list to the above command:

    `--bulk_models [model_a] [model_b]`

#### Docker exec run incremental export

- To incrementally export all models defined in the dictionary `INCREMENT_GENERATOR_HELPERS`
  in file [exporter/incremental_exporter.py](./exporter/incremental_exporter.py):

   ```
   docker exec -it zendesk \
   python run_export.py \
   --project=staging-cityblock-data --dataset=zendesk \
   --bucket=zendesk_export_staging --incremental_export
   ```

- [optional] You can selectively incrementally export a subset of models defined in the dictionary
  `INCREMENT_GENERATOR_HELPERS` by appending this flag and space delimited list to the above command:

    `--incremental_models [model_a] [model_b]`

- [optional] You can incrementally export models from a specific start time or start cursor.

  Query the BQ table [staging-cityblock-data.zendesk.EXPORT_METADATA](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=zendesk&t=EXPORT_METADATA&page=table)
  before selecting start time / cursor values.

  **Note: make sure to apply the `--start_time` or `--start_cursor` flags with specific models defined in the
  `--incremental_models` list, otherwise they will apply to all models by default which you probably don't want.**

  Append only one of the below optional flags to the above `docker exec` command:

  `--start_cursor=[some_string]` or `--start_time=[yyy-mm-dd]`

#### Docker exec publish tickets

- To publish tickets to PubSub run:

  ```
  PROJECT=staging-cityblock-data
  docker exec -it zendesk \
  python publish_tickets.py \
  --zendesk_project=$PROJECT --zendesk_dataset=zendesk \
  --pubsub_project=$PROJECT --pubsub_dataset=pubsub_messages \
  --pubsub_topic=devZendeskTicket \
  ```

- Note quirk that all dev / staging / prod PubSub topics / subscriptions live in project `cityblock-data`.
  The `--pubsub_project` flag refers to the project where published messages will be saved in BQ.

#### Docker exec replay published tickets

- To re-publish previously published messages saved in BQ table [staging-cityblock-data.pubsub_messages.zendesk_ticket](https://console.cloud.google.com/bigquery?project=staging-cityblock-data&p=staging-cityblock-data&d=pubsub_messages&t=zendesk_ticket&page=table),
append the below flags to the above `publish_tickets.py` command:

  `--replay_from_date=[yyyy-mm-dd] --replay_to_date=[yyyy-mm-dd]`

- To replay all messages, you can set the `from_date` to a date in 2019 or earlier and the `to_date` to a future date.

#### Docker clean up

- Once you are done executing docker commands, clean up by running:

  `docker stop zendesk`

### Development

Though you can contribute to zendesk image code and validate using docker run/exec commands,
you may find it quicker to iterate directly in your local python environment.

To set up your local python environment:

- Follow instructions in [containers/README](../README.md#python-environment-setup) to
  setup your python environment.

- Make sure you `pip install -r requirements.txt` and `pip install -r ../python-base/requirements.txt`
