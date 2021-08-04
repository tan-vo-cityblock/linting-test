# Member Service API

The Member Service API allows CRUD operations for Members at Cityblock.
It also serves as the source of truth for member identity.

## Contents

- [Troubleshooting](#troubleshooting)
- [Endpoints](#endpoints)
- [How to](#how-to)
  - [Run on Docker locally](#run-on-docker-locally)
  - [Install NVM, Node, and node modules](#install-nvm-node-and-node-modules)
  - [Set up environment variables](#set-up-environment-variables)
  - [Create a test PostgreSQL database](#create-a-test-postgresql-database)
  - [Connect to a Cloud SQL database via Postico](#connect-to-a-cloud-sql-database-via-postico)
  - [Add Migrations](#add-migrations)
- [Database Entity Relationship Diagram](#database-entity-relationship-diagram)

## Troubleshooting

- [Playbook containing specific strategies (feel free to add to it!)](../playbooks/member_service.md)

## Endpoints

Table of endpoints served by the Member Service

| Method | Endpoint                                         | Description                             | Status   |
| ------ | ------------------------------------------------ | --------------------------------------- | -------- |
| POST   | `/1.0/members`                                   | creates a member                        | complete |
| GET    | `/1.0/members/:memberId`                         | gets a member                           | complete |
| POST   | `/1.0/members/:memberId/externalIds/:datasource` | adds new external ID to existing member | complete |
| PATCH  | `/1.0/members/:memberId`                         | updates member                          | partial  |
| DELETE | `/1.0/members/:memberId`                         | deletes a member                        | complete |

## How-to

### Run on Docker locally

**NOTE** at this time Docker development can only be done with Staging Member Index
In order to develop locally on Docker you must have a `member_service.env` file that contains all the
necessary environment variables (this includes staging credentials).

[Docker for Mac](https://docs.docker.com/docker-for-mac/install/) is required for this

You must then build and run the application:

```
#cd into `member_service` directory where `docker-compose.yml` file is located
docker-compose build  # uses local `Dockerfile`
docker-compose up
```

You should now be able to use the application via `localhost:8080`

Hot-loading is currently not supported, so in order to update the application you must make your source
code changes, save it, then rebuild via `docker-compose build` then serve with `docker-compose up`

### Install NVM, Node, and node modules

First install [Node.js v10](https://nodejs.org/en/) ideally using [nvm](https://github.com/nvm-sh/nvm). Then install node modules.

```
nvm install 10
nvm use 10
node -v # Returns ~10.0
npm install
```

### Set up environment variables

Be sure to set necessary environment variables e.g. through a `~/.bash_profile`.

```
export NODE_ENV=development
export API_KEY=foobar
```

### Create a test PostgreSQL database

Make sure you have Postgres installed. Instead of installing with Brew, you should download [Postgress.app](https://postgresapp.com/downloads.html) and install the version with multiple releases. This allows for super easy switching between versions which is extremely useful when working between Commons and Mixer.

Once you have Postgres installed, you'll want to create an instance of a local database using the following command:

```bash
createdb member_service_test
```

If you installed Postgres from homebrew then you need to run:

```bash
/usr/local/opt/postgres/bin/createuser -s postgres
```

If not, you can continue with the following commands

```bash
psql -c 'create user root with superuser;' -U postgres
psql member_service_test -c "create extension \"uuid-ossp\"; alter database member_service_test owner to root"
npm run migrate
npm run seed
```

### Connect to a Cloud SQL database via Postico

1. Install `cloud_sql_proxy` as described [here](https://cloud.google.com/sql/docs/postgres/connect-external-app).
2. Collect information you need to connect to the database. The Easiest way to collect this information is by decrypting the appropriate `app.*.yaml` file. You can follow the link here for more info on [secret decryption](https://github.com/cityblock/mixer/blob/4140559016da72636b5fb2e366b62e6a9956344e/terraform/README.md#cli---decryption)
   - CloudSQL instance name. For prod, this is currently `cityblock-data:us-east1:member-index`.
   - Database name. For prod, this is currently `prod`.
   - Username. For prod, this is currently `mixer-bot`.
   - Password. You'll need to decrypt the file for that :)
3. Run the proxy to create a connection: `cloud_sql_proxy -instances=cityblock-data:us-east1:member-index=tcp:5433`
   Note that the port above is non-standard, since you probably already have a Postgres
   instance (i.e. the test DB) running locally.
4. In Postico, create a new connection that points to `localhost`
   at the port you specified, using the username/password/database you found earlier.
   It's ok to save the password in the keychain.

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

_NOTE_: if you hit the following error:

```
KnexTimeoutError: Knex: Timeout acquiring a connection. The pool is probably full. Are you missing a .transacting(trx) call?
```

Make sure you're on the correct version of Node (v10)

## Database Entity Relationship Diagram

The Member Service will evolve over time to contain more member related _"facts"_.

As of Sept 2020, here is one generated by [eralchemy](https://github.com/Alexis-benoist/eralchemy), generated by:

```
eralchemy -i 'postgresql+psycopg2://USER:PASS@localhost:5432/staging' -o ../images/member_service_erd.jpg --exclude-tables damm_matrix knex_migrations knex_
migrations_lock
```

![](images/member_service_erd.jpg?raw=true 'Member Service Schema')
