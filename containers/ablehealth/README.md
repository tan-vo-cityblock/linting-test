# Able Health <---> Cityblock

## Data transfer protocol
Able Health is a service provider that provides Cityblock with [HEDIS and risk scores](https://ablehealth.com/hedis-risk/).
 
 This is done by providing Able Health with data [as per their specification](https://docs.google.com/document/d/1zzSY3H3YBQiOHV5ldaKq1u7bPIKPkFPlvuz-6sHQ9YM/edit).
 
 Able Health then processes that data and then makes it available to us via a webservice endpoint.
 It takes approximately 2 days to have this be available.  
 
## What data sources are used to send data to Able Health (data dependencies)?

| BQ dataset | what? |
|---|---|
|`<partner>:gold_claims`|for each of our partners|
|`cityblock-data:medical`|data from our streaming jobs (redox & elation) |
|`cityblock-data:commons_mirror`|for patient demographic information|
|`cityblock-data:common`|for patient index to match back to `commons_mirror`|
|`cityblock-analytics:mrt_commons`|for patient index to match back to `commons_mirror`|
 
## High Level Workflow
There are 2 primary workflows, data delivery and data processing.
 Data delivery can be thought as "Cityblock to Able Health' and data processing can be thought of
 as 'Able Health to Cityblock'
 
#### Cityblock to Able Health
We run a series of queries utilizing the datasets above and create a `CSV` file for each of the
 metrics Able Health requires. This `CSV` is then put into a S3 bucket that Able Health owns and
 uses as a 'drop off point' from Cityblock. We should strive to make data available in the S3 bucket
 by 8PM Pacific time in order to have the results back by the next day.

#### Able Health to Cityblock
Able Health publishes the HEDIS and risk data to a webservice endpoint that is accessible via
 a designated domain for Cityblock. The CSV file containing this data is then loaded into BigQuery.
 If the Cityblock data was delivered to Able Health before 8PM Pacific time the previous day, we can
 expect the results of that will be available at 9AM Pacific time.
 
## Running the Workflow
The data transfer can be run manually by triggering some Python scripts, or in an automated manner as
 a standalone DAG on Airflow. We also need to have the appropriate credentials to complete the
 data transfer.
 
### Authentication
The following table describes the necessary credentials:

|Transfer type| Credentials|
|---|---|
|Cityblock to Able Health|- Running BQ job <br> - BQ read permissions for datasets <br> - Write to GCS bucket <br> - Able Health S3 bucket access & secret key|
|Able Health to Cityblock|- Able Health login <br> - Running BQ job <br> - BQ write permissions for dataset|

To retrieve the `user` and `password`, use the [Secrets script](../../terraform/README.md#cli---updating-encrypted-secrets) 
 to decrypt the secrets stored [in the GCS bucket](https://console.cloud.google.com/storage/browser/cbh-ablehealth-prod-secrets). 
 
For example, 
 ```
# cd to mixer repo
cd scripts/kms
bash cbh_decrypt_from_gcs cbh-ablehealth-prod user ~/tmp-able-secrets/user
 ```
 which will save the username in a file at `/mixer/scripts/kms/user`.

### Locally
After ensuring you have the necessary credentials on your machine as specified above, you can run
 the data transfer via the Python scripts in this project.
 
To run "Cityblock to Able Health", run the following python script:
 ```
 python cbh_to_able.py <BQ_PROJECT> <GCS_BUCKET> <DATESTAMP>
 ```
 Where `BQ_PROJECT` is the Google Project in which to run the BQ queries on. <br>
 `GCS_BUCKET` is the Google Cloud Storage bucket in which to store the files. <br>
 and `DATESTAMP` is the datestamp at which you want to prefix the files under, if not provided it will determine it. <br>

 After this is successful, we must then run the the `gcs_to_s3.sh` script as follows:
 ```
 bash gcs_to_s3 <GCS_BUCKET> <DATESTAMP>
 ```
 that accepts a 2 arguments that is the same as the ones above, this will transfer those CSV files into the 
 Able Health drop off point (S3 bucket).

#### AH to CBH
To run "Able Health to Cityblock", run the following snippets:

Downloading data to Google Cloud Storage
```
USER=... # Able Health user name
PASS=... # Able Health password
DATA_FILE=... # currently one of: measure_results, risk_suspects, risk_scores
curl -u $USER:$PASS https://cityblock.ablehealth.com/exports/patients/$DATA_FILE -L -o /tmp-ah/$DATA_FILE.csv

BUCKET=... # bucket to write data to
DATE=... # date of ingestion
gsutil rsync -r  /tmp-ah/ gs://$BUCKET/ablehealth_results/$DATE
```

Google Cloud Storage to BigQuery
 ```
GCP_PROJECT=...
BUCKET=us-east4-prod-airflow-d4e023bb-bucket  # production bucket
DATASET=...
DATE=... # date of ingested data
python gcs_to_bq.py --project=$GCP_PROJECT --bucket=$BUCKET --dataset=$DATASET --date=$DATE
 ```
 The process will then run and logs will output how much data is appended to the relevant tables
 
### Production (automated)

The "Cityblock to Able Health" is run as a daily job on Airflow. The DAG ID is: `cbh_to_able` and the code for it
 can be found in the [cloud composer project](../../cloud_composer/dags/cbh_to_able.py)
 
The "Able Health to Cityblock" is run as a daily job on Airflow. The DAG ID is: `able_to_cbh` and the code for it
 can be found in the [cloud composer project](../../cloud_composer/dags/able_to_cbh.py)
 
## Scripts
The following helper scripts are provided

Script|Function
---|---
`view_ah_s3_bucket`|View the contents of the Able Health S3 bucket
`gcs_to_s3`|Sync files from a specific location on GCS to Able Health S3 bucket

## Fake Able to Seed QM Service Staging - seed_qm_service_staging.py

**DO NOT RUN SCRIPT IN PROD ENV**

Script for resetting Quality Measure Service Staging db table `quality_measure.member_measure` with fake able data using Commons staging member ids.

### Purpose
Commons [env] is a client of the QM Service [env] and changes to the QM Service should not break how QM data is 
used/displayed in Commons.

QM data is initially generated (via Able) and is only available for prod members.

The purpose of this script is to fake Able data for Commons staging members and make it available for dev/staging 
environments in QM Service and Commons, and for CI/CD before releases to prod. 

For an example, see the [quality table](https://commons-staging.cityblock.com/members/quality) in Commons staging.

Refreshing QM Service staging data does not need to occur frequently and should only need to happen when updates are 
made to the QM Service that will affect Commons (i.e. adding quality measures for a new market).
 
Additionally, given the limitations of staging (i.e. we can't replicate day-to-day continuity of Able prod results), faking is implemented to capture state at a given fake point in time.

For purposes of development, there has not been a need to reflect the continuity of Able prod state in staging. 

So, there is no need to run this script as frequently as we process Able prod data.

### Running Script
- See ["Refresh QM Service Staging DB" section in QM Service README](../../services/quality_measure/README.md#refresh-qm-service-staging-db) for instructions on running the `seed_qm_service_staging.py`