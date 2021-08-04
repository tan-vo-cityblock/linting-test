# Health Events Service

## Table of Contents

- [Overview](#overview)
- [Tools and Access](#tools-and-access)
- [Setup](#setup)
- [Run Health Events Service with Commons locally](#run-health-events-service-with-commons-locally)
- [How to test pubsub locally](#how-to-test-pubsub-locally)
- [Connect to a Cloud SQL database via Postico](#connect-to-a-cloud-sql-database-via-postico)
- [Refresh the facility_map_cache table](#refresh-the-facility-map-cache-table)
- [Add Migrations](#add-migrations)
- [Updating a view with a Migration](#updating-a-view-with-a-migration)
- [How to deploy to Production](#how-to-deploy-to-production)

### Overview

The Health Events Service exposes a graphql endpoint for a member's health events. These events are only accessible through the Apollo Federation Gateway that lives in Commons. There is a Pub/Sub consumer that ingests health events from RedoxSubscriber and sends a POST request to the `/hie-messages` endpoint to save them. This is how we are currently ingesting health events to the service.

### Tools and Access

#### **VS Code (or equivalent)**

If you have not done so yet, you will need to install a code editor. We recommend [VS Code](https://code.visualstudio.com/download).

#### **Node and `npm`**

In your terminal, see if you already have Node installed by running `node --version`. If you see something like `bash: node: command not found` you'll need to first [install Node](https://nodejs.org/en/download/). After following the install instructions, run `node --version` again. It should output something like `v14.15.1`. `npm` (Node Package Manager) ships with your Node install, but you'll need to activate it. From your terminal, run `npm install npm -g`

#### **`nvm` (Node Version Manager)**

Visit the [nvm Github page here](https://github.com/nvm-sh/nvm#installing-and-updating) and execute the `curl` script to install the `nvm` executable. Once you have it, reference our [package.json](package.json) file. Under the `"engines"` property, you'll see the service's Node version. Install that Node version with `nvm install <version number>` and then `nvm use <version number>`. To see if these worked correctly, run `node --version` in Terminal, to see which Node version you're using.

#### **Postgres App and Postgres CLI**

Follow installation instructions for [Postgres.app](https://postgresapp.com/) and [Postgres CLI](https://postgresapp.com/documentation/cli-tools.html). Once installed, create a root permission with `psql -c 'create user root with superuser;' -U postgres`. If your terminal cannot find the `psql` command, quit the Terminal application, open it again and try again.

#### **Postico**

Install [Postico](https://eggerapps.at/postico/) as your database console for locally run queries.

### Setting up the service

- Open a new Terminal shell, navigate to the [mixer](https://github.com/cityblock/mixer) repo and `cd services/health_events_service/`
- Setup Postgres Database
  - For dev: `psql -c 'CREATE DATABASE health_events_service;' -U root postgres`
  - For tests: `psql -c 'CREATE DATABASE health_events_service_test;' -U root postgres`
- Create `.env` file [Copying from this OneLogin secure note](https://cityblock.onelogin.com/notes/127899)
- `npm install`
- `npm run migrate`
- Populate your local database
  - `npm run backfill-hie-messages:dev`
- `npm run dev`

### Run Health Events Service with Commons locally

You can have Commons make requests to the Health Events Service locally by doing the following:

**Health Events Service Steps**

- Follow the [Setup](#setup) steps listed above
  - If you already created your Postgres Database, created a `.env` file and populated your local database already then feel free to skip those steps.
- Once you run `npm run dev`, make note of the port that was outputting in the terminal. You might need to change the port to `8081` since Commons runs on `8080` locally. In order to do this, set `PORT=8081` in your Health Events Service `.env` file. Don't forget to restart the app if you updated the `.env` file while its running!

**Commons Steps**

- In your Commons `.env` file, add `HEALTH_EVENTS_SERVICE_GRAPHQL_ENDPOINT=http://localhost:8081/graphql`.
- Start Commons via `npm run dev`

### How to test pubsub locally

To get started, install the [Google Cloud SDK][] along with the [JDK][] (verision 7 or 8).

Next, install the [Google PubSub Emulator](https://cloud.google.com/pubsub/docs/emulator) by running the following commands:

    gcloud components install pubsub-emulator
    gcloud components update

The following instructions assume you have a recent version of Ruby installed, if not install Ruby with [Homebrew](https://formulae.brew.sh/formula/ruby#default):

First, ensure that you have the google-cloud gem installed:

    gem install google-cloud

Then, run:

    gcloud beta emulators pubsub start

In a fresh Terminal tab, run:

    $(gcloud beta emulators pubsub env-init)

Next you will need to run the consumer for Health Events Service locally. First you will need to set two environment variables so that the PubSub script knows to connect to the emulator. The port number should be provided by the emulator output of running `gcloud beta emulators pubsub start`.

    export PUBSUB_EMULATOR_HOST=localhost:8432
    export PUBSUB_PROJECT_ID=my-project-id

Then run the command `npm run dev`.

In a new tab, open an IRB session, and execute the following commands, substituting data where required:

```
  require 'google/cloud/pubsub'
  pubsub = Google::Cloud::Pubsub.new(project_id: 'cityblock-data')
  # Feel free to change the topic/subscription names if you are testing a different endpoint
  topic = pubsub.create_topic('devRawHieEventMessages')
  subscription = topic.create_subscription('devRawHieEventMessages')
  subscription.push_config do |pc|
    # POST to the local /hie-messages endpoint
    pc.endpoint = "http://localhost:8081/hie-messages"
  end
  # This can can whatever you'd like, but this is a sample payload to test
  data = '{"eventType":"Notes.New","messageId":1010,"patientId":"test-id","payload":{}}'
  topic.publish(data)
```

**Note:** The topic and subscription name needs to be the same.

**Note:** If you get null/nil/false as the return value of any line above, you are not connected to the emulator.

### Connect to a Cloud SQL database via Postico

1. Install `cloud_sql_proxy` as described [here](https://cloud.google.com/sql/docs/postgres/connect-external-app).
2. Collect information you need to connect to the database. The Easiest way to collect this information is by decrypting the appropriate `app.*.yaml` file. You can follow the link here for more info on [secret decryption](https://github.com/cityblock/mixer/blob/4140559016da72636b5fb2e366b62e6a9956344e/terraform/README.md#cli---decryption). An alternate approach would be to view the secret's value directly in Google's Secret Manager in the `cityblock-secrets` project.
   - CloudSQL instance name. For staging, this is currently `cbh-services-staging:us-east1:services`.
   - Database name. For staging, this is currently `health_events`.
   - Username. For staging, this is currently `health_events_service`.
   - Password. You'll need to decrypt the file for that :)
3. Run the proxy to create a connection: `./cloud_sql_proxy -instances=cbh-services-staging:us-east1:services=tcp:5433`
   Note that the port above is non-standard, since you probably already have a Postgres
   instance (i.e. the test DB) running locally.
4. In Postico, create a new connection that points to `localhost`
   at the port you specified, using the username/password/database you found earlier.
   It's ok to save the password in the keychain.

### Refresh the facility map cache table

`facility_map_cache` is a cache of the [HIE Facility Master Production spreadsheet](https://docs.google.com/spreadsheets/d/1c99dgIPNXRITMLvuUWfKSp5yLbr4JmCM9pkNJ0g53ZM/edit#gid=0). Because this sheet is fairly stable, there is no auto-refresh. Whoever is querying the table will need to occasionally manually refresh it. To do that, follow these steps:

1. Make sure you are [set up to run Health Events Service locally](#tools-and-access)
2. Connect to the Production CloudSQL instance from your local machine. [Instructions here](#connect-to-a-cloud-sql-database-via-postico)
3. Navigate to the `health_events_service` directory and run `npm run refresh-facility-map-cache:production`
4. Confirm a successful refresh by checking the table in Postico

### Add Migrations

1. To add a new migration, run the following:
   ```
   npm run migrate:make migration_name // ex npm run migrate:make add_state_insurance
   ```
2. Once the file is auto-generated, locate to the newly created migration file and populate the `up` and `down` functions
3. To test that the migrations execute correctly, make sure that the following commands execute correctly
   ```
   npm run migrate
   npm run migrate:rollback
   ```
4. Finally, run on development environment: `npm run dev` to ensure migrations are successful

### Updating a view with a Migration

We are versioning the views created in migrations so its easier to maintain updates. This system is based off of [this article](https://medium.com/@j3y/beyond-basic-knex-js-database-migrations-22263b0fcd7c).

When creating a _new_ view:
- Create a directory with the view name in `./db/views` (i.e `./db/views/hie_message_signal`)
- Create a file named `v1.ts` that exports up and down functions. The `up` function should represent the SQL required to initially create the view. The `down` function should represent the dropping of the view.
- Add a migration (`npm run migrate:name example_migration_name`) and use the v1 up/down functions.

i.e in the migration file
```
import { Knex } from 'knex';
import * as hieMessageSignalV1 from '../../db/views/hie_message_signal/v1';

export async function up(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV1.up);
}

export async function down(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV1.down);
}
```

When updating an _existing_ view:
- In the `./db/views/view_name` directory, add a new file with the incremented version name (i.e `v2.ts`). The `up` function should represent the SQL for the newly updated view logic. The `down` function should *import and use the previous version's up function`.

i.e in v2.ts
```
import * as v1 from './v1';

export const up = `
  DROP VIEW IF EXISTS "example_view";
  CREATE OR REPLACE VIEW "example_view" AS (SELECT * FROM "examples");
`;

export const down = v1.up;
```

- Add a migration (`npm run migrate:name example_migration_name`) and use the new version's up/down functions.

### How to deploy to Production

In order to deploy to production, you must cut a new release in [mixer](https://github.com/cityblock/mixer) with the naming convention of `health-events-service-X.X.XX` (Replace X.X.XX with the proper version).

Here are the steps to take:

- Confirm that the build on master is passing on the latest stage deployment/build, especially tests. This can be viewed in the cityblock-data project of [Cloud Build](https://console.cloud.google.com/cloud-build/builds?project=cityblock-data). This is due to not having a run tests step for production releases.
- Navigate to the [mixer releases view in Github](https://github.com/cityblock/mixer/releases)
- Determine the latest version for Health Events Service. i.e: `health-events-service-x.x.xx`
- Click `Draft a new release`
- Add the tag version with the naming convention `health-events-service-x.x.xx`. Please note the x.x.xx should be replaced with the new version.
- Provide an accurate title & description of what the release entails. The title should be prefixed with Health Events Service and the release version .
- Click `Publish release` when you're ready
- From there, navigate to the cityblock-data [Cloud Build project](https://console.cloud.google.com/cloud-build/builds?project=cityblock-data) and confirm the latest release was successful.

**Notes/Caveats**

- If the GraphQL schema is updated in the Health Events Service, make sure you test/QA with Commons locally to confirm Apollo Federation doesn't error upon start. This can cause the GraphQL endpoint to return a 500 error due to a mismatch in schemas. If you do release a new version of Health Events Service that updates the GraphQL schema, then you should follow up with either a deploy of the latest version of Commons or restarting Commons so it can fetch the latest schema.
