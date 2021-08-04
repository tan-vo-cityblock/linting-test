# Q-Reviews --> Cityblock

## Data transfer protocol
Q-Reviews is a service provider that provides Cityblock with NPS data. Another service sends
q-reviews member info (phone numbers, etc) and a trigger after a visit. With that trigger
q-reviews then sends the member a survey. Q-reviews provides an enpoint to retreive the results
of the surveys. The documentation for their endpoint can be found [here](https://qrev.ws/api/client/v2/response/docs).

## High Level Workflow

#### Q-Reviews to Cityblock
Q-Reviews publishes the survey data to a webservice endpoint that is accessible via
 a designated domain for Cityblock. The process is as follows:

 1. Query bigquery to get the largest `sequence` value in the database. `sequence` is a
 monotonically increasing integer assigned by qreviews with every addition or update to an
 object.
 2. Send a GET request to the qreviews results enpoint using the `sequence__gt` (sequence greater than)
 query paramater. This will retreive all objects from qreviews that have been updated since the
 last object we have in our database.
 3. Convert all key names in the response data from snake case to camel case.
 4. Create the dataset if it does not exist
 5. Create the table if it does not exist
 6. Stream the data into bigquery

## Running the Workflow
The data transfer can be run manually by triggering some Python scripts, or in an automated manner as
 a standalone DAG on Airflow. We also need to have the appropriate credentials to complete the
 data transfer.

### Authentication
To run the job you need the following credentials credentials:

api_account_sid: unique id for our account
api_key: key for our account

### Locally
After ensuring you have the necessary credentials on your machine as specified above, you can run
 the data transfer via the Python scripts in this project.

To run "Q-Reviews to Cityblock", run the following python script:
 ```
 python qreviews_to_bq.py <PROJECT> <DATASET> <api_account_sid> <api_key>
 python qreviews_to_bq.py --project cbh-spencer-carrucciu --dataset qreviews --api-account-si AAAAAAA --api-key BBBBBB
 ```
 Where `PROJECT` is the BQ project we are reading from and writing to<br>
 Where `DATASET` is the dataset to upload the data to in bigquery<br>
 Where `api_account_sid` is account sid for https://qrev.ws/api/client/v2/<br>
 and `api_key` is the api key for https://qrev.ws/api/client/v2/<br>

 Alternatively we can have a `secrets.yml` file available in the root of the directory which contains a `api_account_sid`
 and `api_key` with the credentials (this is how it is handled in production)

 The process will then run and logs will output how much data is appended to the relevant tables


### Production (automated)

The "Q-Reviews to Cityblock" is run as a daily job on Airflow. The DAG ID is: `qreviews_to_cbh_vX` and the code for it
 can be found in the [cloud composer project](../../cloud_composer/dags/qreviews_to_cbh_v1.py)
