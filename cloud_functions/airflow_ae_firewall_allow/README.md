# Airflow and App Engine Firewall Update Service

## What this does

This Cloud Function is a HTTPS URL that is invoked to check and update firewall rules
for a given App Engine instance if need be.
Based on the request body, it gathers the IP addresses of the Airflow cluster and checks
it against the App Engine instance to ensure they are allowed. If they are, then this function
does nothing. If there is a mismatch, there are updates made to ensure all the machines
in the cluster can send successful requests to the App Engine instance.

## Deploying

Terraform instance contains Cloud Function definition and information associated with the deployment of it
should any changes occur in the code, and then it is merged into `master`
The deployment must be done manually post-merge via the following command:

```
gcloud functions deploy airflow_ae_firewall_allow \
--source https://source.developers.google.com/projects/cityblock-data/repos/github_cityblock_mixer/moveable-aliases/master/paths/cloud_functions/airflow_ae_firewall_allow \
--runtime python37  \
--trigger-http \
--region=us-east4 \
--project=cityblock-data
```

## Dependencies

The function currently uses [libraries that are already provided in the Python runtime](https://cloud.google.com/functions/docs/writing/specifying-dependencies-python#pre-installed_packages)
as well as the ones defined in the [`requirements.txt`](./requirements.txt)

## Debugging

Start at Google Cloud Logging - [here is a starting point for the log query](https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_function%22%20resource.labels.function_name%3D%22slack_platform_service%22%20resource.labels.region%3D%22us-east4%22?project=cityblock-data)
