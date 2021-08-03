# `spark`

Welcome to [Spark](https://spark.apache.org/) at Cityblock!

## Environment setup

All of the commands in this section should be run from the `spark` directory.

If you run into errors running these commands, check the [gotchas](#gotchas) section before reaching
out to #spark-eng for troubleshooting.

### Installing `pyspark` and dependencies

Just run `./install.sh`. That's it!

### Set up BigQuery reads and writes

Run `./gcloud_setup.sh`. That's it!

### Verify your installation

Run

```bash
./pyspark_submit_local bigquery_io_test.py | tee bigquery_io_test.log
```

If your final few lines of output include

```
Successfully wrote to and then read from BigQuery!
```

then everything is working as intended!

If the test throws an exception or otherwise fails, ping #spark-eng and include the
`bigquery_io_test.log` file generated when you ran the test command.

### Accessing data outside of your personal project

By default, `gcloud_setup.sh` only gives you the ability to read and write from your own personal
project (these permissions are tied to a separate service account created by the script instead of
your user account). The platform team is working on a solution that will seamlessly grant read
permissions to relevant production projects.

For now, if you need to access PHI, drop a message in #spark-eng along with the output of

```bash
source .env && echo $CBH_SPARK_SVC_ACCT_EMAIL

```
   
### Gotchas

#### `pip install` is causing errors when running `install.sh`

Manually install pip packages in the below order:

```bash
pyenv activate env-spark
pip install pypandoc==1.4
pip install -r ~/mixer/spark/requirements.txt
pyenv deactivate
```
And then re-run the `install.sh` script.

#### `install.sh` is taking a long time
If you haven't run `brew` in awhile, it needs to fetch the entire catalog of available
packages. This can take several minutes. Don't worry, nothing is broken! You'll just have to wait a
bit.


#### `gcloud_setup.sh` fails with `hashlib` in the error message
This probably means that you need to [upgrade to OSX
Catalina](https://discuss.bitrise.io/t/broken-python-2-7-hashlib-in-new-xcode-10-3-x-mojave-stack/11401).

You can verify that this is the problem by checking whether you are still on OSX Mojave and running
```bash
pkgutil --pkg-info=com.apple.pkg.CLTools_Executables | grep version
```
If the number starts with `11.3` then you almost certainly need to upgrade your OS.
   
## Using `pyspark` locally

Do **NOT** use `pyspark` directly.

Instead, use `pypark_local` to run a local pyspark shell, and `pyspark_submit_local` to run jobs on
locally.

While a shell is open or a job is running, you can view your local spark cluster's web UI by
navigating to `localhost:4040` in your browser. This is very useful for debugging failed jobs or
checking on long-running ones.
   
## Running jobs on `gcloud dataproc`

**This section is a work in progress. If you need to run dataproc jobs right now, reach out to
#spark-eng.**

### Setup

*TODO: This needs to be fleshed out in gcloud_setup.sh.*

1. Enable the Dataproc API for your personal project.
1. Grant your project's default compute service account the following permissions in all projects
   you need to **read** from.
   
   - BigQuery Read Session User
   - BigQuery Job User
   - BigQuery Data Viewer 
   
   And these permissions for all the projects you need to **write** to
   
   - BigQuery Job User
   - BigQuery Data Editor
   
   The default compute service account is the service account used by
   Dataproc workers, who need the above permissions to do their jobs.

### Example job

Before dataproc image version 1.4, the default python version is 2. We *must* use image version 1.4
to get a default python version of 3.

Here's an example: creating a dataproc cluster and running the `distinct_count.py` job.

1. Switch to your gcloud project of choice.
1. Grant your project's default compute-engine service account the following roles: BigQuery Data
   Editor, BigQuery Read Session User, BigQueryJob User (the default compute-engine service account
   is automatically granted the Dataproc Worker role)
1. Create a dataproc cluster with image version 1.4 (the only dataproc image version with python3)
   ```bash
   gcloud dataproc clusters create python3-test \
     --image-version 1.4 \
     --bucket cbh-ben-barg-tmp \
     --master-machine-type n1-standard-4 \
     --num-workers 2 --worker-machine-type n1-standard-4 \
     --region us-east4
   ```
   This cluster will use your project's default compute service account, which by default has
   editor access to any bucket in your project.
1. Submit the job
   ```bash
   gcloud dataproc jobs submit pyspark --cluster python3-test --region us-east4 \
                                       --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
                                       --py-files cbh_setup.py distinct_count.py -- \
                                       --source_dataset gold_claims_cci_facets_3c6ce1f3 \
                                       --source_table MemberV2_20200101 \
                                       --field eligibility.lineOfBusiness \
                                       --destination_dataset spark_test \
                                       --destination_table distinct_count_dataproc_0
    ```
    Here's what each of the arguments are for:

    - `--cluster` and `--region` are required to specify the cluster
    - `--jars` includes the BigQuery spark connector library
    - `--py-files` includes any local dependencies (this job only uses our `SparkSession` setup
      module)
   
   Any argument after the `--` is passed to the python job itself.
1. Delete the cluster now that your job has run successfully
    ```bash
    gcloud dataproc clusters delete --region us-east4 python3-test
    ```

## Future work

Our spark infrastructure is still very bare bones. Here's a list of questions to answer and things
to improve. This are focused around making `pyspark` usable for DSA.

### Developer experience
- tutorial for simple tasks: BigQuery IO, File IO (including Parquet)
- tips for faster iteration speed: sampling dataframes, caching locally
- test framework and guide

### Cloud resourcing
- automatic setup for dataproc worker service account permissions
  + including an efficient way to specify read/write access across various bigquery projects (which
    touch multiple terraform states)
- philosophy around on-demand versus persistent spark cluster
  + should all users be able to spin up dataproc clusters?
  + should we place limits on user-created clusters?
- more secure solution for gcs-connector service accounts (potentially use `cbh_setup` to access
  cloud kms for the service account's keyfile)
  + alternatively, see if it's possible to use OAuth
  + or, maybe we should write to Parquet instead and upload to BQ separately

### `pyspark` shims
- seamless way to submit jobs to dataproc that have complex dependencies across the project
