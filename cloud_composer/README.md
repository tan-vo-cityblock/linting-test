# Cloud Composer

Project containing DAGs and plugins for Cityblock Cloud Composer (Airflow) instance.

## Quickstart

### Production
- [Airflow Web UI](http://airflow-prod.cityblock.com)
- [Composer UI on GCP (Dashboard)](https://console.cloud.google.com/composer/environments/detail/us-east4/prod-airflow/monitoring?project=cityblock-orchestration)

### Testing
- [Local Development]
- [Airflow Web UI](https://ub16b5f6f1ad484c4-tp.appspot.com/)
- [Composer UI on GCP (Dashboard)](https://console.cloud.google.com/composer/environments/detail/us-east4/cityblock-composer-env-test/monitoring?project=cityblock-orchestration)

### Support
- `#platform-support` Slack channel
- [Airflow Documents](https://drive.google.com/drive/folders/1LtJKn2h7ehCSxg7zwzYwQKhBj_tsLtxO)

## Contents
- [What is Cloud Composer?](#what-is-cloud-composer)
- [How to make a DAG](#great---now-how-do-i-make-a-dag)
- [DAG checklist](#wow-that-was-easy-but-how-do-i-contribute-it-to-our-cloud-composer-environment)
- [Testing](#testing)
- [FAQ](#faq)
- [Mirroring DAGs](#mirroring-dags)
- [Coming Soon](#coming-soon)

## What is Cloud Composer?
[Cloud Composer is a managed service by Google Cloud](https://cloud.google.com/composer/docs/) 
that is built on top of [Airflow](https://airflow.apache.org/).
It allows you to create, schedule, monitor, and manage workflows. 
A workflow can be described as a Directed Acyclic Graph (DAG), 
[Airflow has a great primer on how it works](https://airflow.apache.org/concepts.html#dags).

A DAG can be described at a high level as a series of Extract, Transform, and Load (ETL) processes. 
A natural starting point is obtaining data as your input (Extract), apply some custom logic to it 
(Transform) and finally store or publish those results (Load).

For example, a single [Scio](https://spotify.github.io/scio/index.html) job that consists of 
reading data from BigQuery, doing a set of 
[transforms](https://beam.apache.org/documentation/programming-guide/#core-beam-transforms), then 
storing the results to Google Cloud Storage can be a single ETL. 
Running a series of Scio jobs (that can have independent branches of other Scio jobs or 
entirely different ETLs) can be defined as a DAG.

## Great - now how do I make a DAG?
When contributing a DAG, you should ask yourself the following questions:
1. Does this workflow make sense to add into Airflow (specific features utilized)?
1. How idempotent is the workflow (do I need to specify external parameters each time I run it)?

If unsure on how to answer these questions, please consult your specific cases with `#data-eng`.

If you have a good case for making a DAG, go ahead and do so utilizing the
 [`KubernetesPodOperator`](https://airflow.apache.org/kubernetes.html).
This operator is ideal because it allows us to have the task be truly standalone by being its own image.

Please see the example in [`/dags`](dags/example_scio_v1.py) which is a DAG containing 2 Scio jobs in this format.

## Wow that was easy! But how do I contribute it to our Cloud Composer environment?
Complete the following checklist to add a DAG (applies primarily for `KubernetesPodOperator`)
- [ ] ensure all tasks in the DAG can be containerized (examples in [`/containers`](../containers))
- [ ] have relevant container images available on 
[Cityblock Data GCR](https://console.cloud.google.com/gcr/images/cityblock-data) 
- [ ] test the DAG on our testing environment (see [testing](#testing))
- [ ] create a PR containing:
  - DAG Python file in `/dags`
  - Service account for DAG with appropriate roles to necessary resources

## Testing
Before deploying your DAG to the production environment, it should be run in our test environment.
[Ensure you are authenticated with the Kubernetes Clusters](../scripts/auth/gke-cityblock-orchestration)

(TODO - flesh this out more)
This directory (`mixer/cloud_composer`) is written as if it is a python module called "dags". We are currently using `$ tox`  as a test runner (wrapping `pytest`). `$ tox` should run all tests written in `tests/`.

Make sure airflow is installed locally, then modify `~/airflow/airflow.cfg` to have

```cfg
[core]
dags_folder = /Users/to_your_mixer/mixer/cloud_composer/dags
...
[tests]
unit_test_mode = True

```


### General workflow
You should upload your DAG and run it with IO specified to sandbox locations as soon as you can (using `upload_dag`).
 If you update the `airflow_utils.py` file - ensure you upload that as well using that same script.
 Ensure you also copy the k8s secret should it be applicable to your DAG using the `copy_secret` script.
 This allows for an iterative development cycle and as the environment more or less mimics production, allows
 us to catch all sorts of errors sooner rather than later (after deploying to prod).

Furthermore, using a sandbox locations for IO (for example, a BQ query writing to your sandbox project) on the tasks
 in the DAG prevent running non-code reviewed items in the production environment.

At this point the DAG should be visible in the test environment UI - you can manually trigger a run and see how the DAG
 behaves. Iterate on all the necessary pieces based on what logs tell you for the DAG (successfuly, failed, etc.). You
 can also use `kubectl` to get additional details from the relevant pods that are deployed for the DAG.

Once all testing is complete, it is best practice to remove the DAG and secrets from the test environment.
 Do this with the appropriately named `remove_dag` and `remove_secret` scripts.
 
## Plugins
### Hooks
[Hooks are interfaces with external platforms](https://airflow.apache.org/docs/stable/concepts.html#hooks)
that give the Airflow environment a means of connecting to them.

[If a Hook is not already provided in our version of Airflow](https://airflow.apache.org/docs/stable/_api/index.html#hooks-packages)
, this would be a good time to have one of our own. You can look at an
[existing example we have (Pagerduty)](/plugins/hooks) and see it's usage
in the [utils file](/dags/airflow_utils.py). [Here is also a great guide
to getting started with custom Hooks and Operators](https://www.astronomer.io/guides/airflow-importing-custom-hooks-operators/)

## FAQ
#### But I don't like the `KubernetesPodOperator`, can I use something else?
While Airflow offers many different kinds of operators for specific use cases, there are a number of things
to consider when filling your DAG with a large variety of them.
This may introduce more complexity and difficulty in maintaining the large variety of operators.
By limiting ourselves to a small number of operators, in this case only one, it allows debugging 
and updates to be handled in a uniform manner.

[This article highlights these points and more in greater 
detail](https://medium.com/bluecore-engineering/were-all-using-airflow-wrong-and-how-to-fix-it-a56f14cb0753).

With that said, we can make exceptions on a case by case basis with the caveat that these operators 
will **not** be maintained by `#data-eng` (i.e., when debugging or digging into internal behavior).

#### What is a service account and how do I know what permissions/roles it needs?
If you haven't already, it is now time for you to get acquainted with Cloud IAM 101. A 
[service account in GCP](https://cloud.google.com/iam/docs/understanding-service-accounts), 
in this context, is an 'identity' that the DAG utilizes which dictates the services and resources in a given
Google project can access. For example you may have a DAG query for some data in a BQ table contained in project A,
 then run a Python script with said data in project B, and finally store results to a bucket again in project A. 
 The service account for this DAG would need read access to the BQ table in project A, run on a compute instance for
 project B, and finally write to GCS in project A.

For a given service, all the 
[predefined roles are available here](https://cloud.google.com/iam/docs/understanding-roles).
We will be using these for the time being, **but** in the future we will implement more granularity to 
the service accounts for each DAG (eg. bucket prefixes, BQ datasets/tables, etc.).

This is ultimately codified into 
[`/terraform/cityblock-orchestration/svc_acct_delegation`](../terraform/cityblock-orchestration/svc_acct_delegation.tf)
 in the form of a `module` per service account. The `google_project_iam_member` would represent each role that is
 necessary for the service account. Please see the example in which a service account was created for 
 [`example_scio`](dags/example_scio_v1.py).

#### My pod is in 'pending' state for a long time and it just times out, I don't know why
The Airflow UI doesn't always provide the best logs for a Kubernetes pod out of the box and
 there are [issues with it working](https://issues.apache.org/jira/browse/AIRFLOW-4526).
Because of this, you can run some manual commands to better debug your pod.
For starters, bookmark this
 [`kubectl` cheat sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/) if you are not
 familiar with Kubernetes.

Once a pod is deployed to your GCP k8s cluster, you can 
[view it on the UI to see it's state and logs](https://console.cloud.google.com/kubernetes/workload).
You can also utilize the `kubectl get pods` to find the pod of interest (will be the `name` with a hash
 appended), and using that you can do `kubectl describe pods <NAME OF POD>` or even `kubectl logs -f <NAME OF POD>`.
It is also worth deleting the pods once you are done investigating them and no longer need them
 for reference, as the pods list can quickly get bloated.

#### I need a specific Python library for my DAG (ex. `numpy`, `pandas`, etc.)
When creating your PR, include the PyPi dependency in 
[the terraform module](../terraform/cityblock-orchestration/main.tf) under `pypi_packages` for the 
`google_cloud_composer` resource:
```
resource "google_composer_environment" "cloud_composer" {
  ...
  config {
    ...
    software_config {
      ...
      pypi_packages {
        numpy = "==1.16.3"
        # add your package(s) here
      }
    }
  }
```
Please specify the version as well, there is an example already available there for reference. 
For additional details see 
[Terraform 
documentation](https://www.terraform.io/docs/providers/google/r/composer_environment.html#pypi_packages)

#### I don't see my DAG/plugin on the Airflow UI - what gives?
The DAGs/plugins in the Airflow UI is tied to what is present in the relative locations of the 
Cloud Composer storage bucket.
The `cloudbuild.yaml` file in this project achieves a sync of the code repository items to the 
bucket once triggered.
This trigger is done when a PR is accepted to the `master` branch.
In other words, once your PR containing your DAG is merged to `master`, you will then be able to 
see your DAG within minutes on the UI.

#### This is all great - but I still have questions
Great! Reach out to `#data-eng` for assistance.

### Mirroring DAGs

If you would like to create a DAG to mirror a database from a Cloud SQL instance, please use the [script for mirroring 
the staging member index](dags/mirror_staging_member_index_v1.py) as a template. In general, the only things you should need
to change are: 
1. The config variables specified at the top of the file
2. The secrets stored in K8s
3. The `id` and `start_date` of the new DAG

The file should be named `mirror_{env}_{cloud_sql_instance}.py`.

Specifically for Commons Mirror, we need to use a personal access token to clone the Commons repository in order to 
export and mirror builder tables (that are stored as code). The access token is passed in as a k8s secret. The 
permission set for the token is `repo` level access, specifically for the `public_repo` permission which allows
repository access. In the future, we should probably [use an ssh-key in conjunction with the service account
running the mirror to access the repository](https://jira.cityblock.com/browse/PLAT-1268).  

### Coming Soon
- Guide on [Sensors](https://github.com/apache/airflow/tree/master/airflow/sensors) and 
[Hooks](https://github.com/apache/airflow/tree/master/airflow/hooks); how to use them
- More examples of DAGs showcasing things beyond Scio jobs
- Details on using operators outside `KubernetesPodOperator`
