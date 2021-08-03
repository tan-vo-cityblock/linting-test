# Cloud SQL Mirroring

See [EDD](https://docs.google.com/document/d/14XnzqcueuHyOGtM9W2gjElS5frg39bjTHTyo04-HC_Q) for full context of the problem we're solving here.

## Script to List Tables to Mirror

On Airflow, each mirroring pipeline corresponds to database on a Cloud
 SQL instances. This script gathers all the names of tables on that
 database in question. The names are important to funnel into the next
 step as they will be used to in the actual Cloud SQL export commands.

To connect to the database, we need to go through the Cloud SQL proxy 
 which connects to our Cloud SQL instance through a local port (default
 for Postgres is `5432`). The adapter we are using to connect through 
 the proxy is SQLAlchemy. The credentials used for the connection are
 mounted on the container by Kubernetes; all we need to do is load and
 use them in the connection.
 
Once the database engine (i.e. the connection pool) has been created, we
 can use the `table_names()` function to list out all tables associated
 with the database connected to. We push the relevant table names out to
 a `return.json` file that eventually makes its way to future airflow
 steps.  

## Script to Export Tables to GCS

This script can be thought of as a wrapper to a set of `gcloud sql export`
 calls, operating on all of the tables that we have listed out in the
 above script. For each table, we export out a schema file (containing
 relevant metadata around column names, data types, nullability) as well
 as a data file (containing the actual data itself). The exports are sent
 to a GCS location provisioned by Terraform. Each file is suffixed by a
 shard date that can either be passed as an argument or defaulted as the
 date when the script is run.
 
## Script to Update BQ Views

Once the Scio job to mirror the Cloud SQL exports on BQ has finished, we
 need to update the existing table views to point to the latest sharded
 table. The output tables from the mirroring job are all suffixed by
 the year/month/day. It's important from a consistency standpoint to
 provide views for all of these tables such that our consumers do not
 have to constantly update their queries to account for the latest date.  