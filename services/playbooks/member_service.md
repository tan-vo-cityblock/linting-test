# Member Service
HTTPS REST API allows CRUD operations for Members at Cityblock.

## What is this service, and what does it do?
The Member Service is a microservice that allows clients to interact with member data stored in the member index
 (a CloudSQL managed Postgres database containing various member related data).

By and large, this mostly implies CRUD operations on member insurance or member demographic data, but we also have
 ancillary routes such as publishing member data through Pub/Sub to Commons. 

The eventual goal with the Member Service is to have the member index be the source of truth for all member data
 and the service itself to act as the communication layer to that data.

## Who is responsible for it?
Platform team (Member Management workstream)

## What dependencies does it have?
Internal Services: Secrets Management on KMS

GCP services: App Engine Standard, Cloud SQL, Cloud Build, Pub/Sub, and KMS

## What does the infrastructure for it look like?
App Engine hosts the Node.js application and Cloud Build handles deployments to both the staging and production environment
 via triggers. There is a single HA Postgres Database on Cloud SQL that is connected to the application. 
 During deployment there is a temporary connection made to the database for the sake of running migrations.
 [See section on how the configuration YAML is altered](#changing-app-engine-config-yaml)

## What metrics and logs does it emit, and what do they mean?
### Metrics
- [Cloud Monitoring Dashboard](https://console.cloud.google.com/monitoring/dashboards/custom/10934129813431789033?project=cityblock-data)
- [App Engine dashboard view](https://console.cloud.google.com/appengine?project=cbh-member-service-prod&serviceId=default)
- [Deployment history](https://console.cloud.google.com/cloud-build/builds?query=trigger_id%3D%22b92dad8b-a677-4749-956e-ac6eff12271c%22&organizationId=250790368607&project=cityblock-data)

### Logs
- General application logs, tail via: `gcloud app logs tail --project cbh-member-service-prod`
  - Staging logs are in a different project: `gcloud app logs tail --project cbh-member-service-staging`
- Available on Cloud Logging in specific project

## What alerts are set up for it, and why?
- Production deployments that fail would be surfaced on #eng-deployments Slack channel.

## Tips from the authors and pros
### Changing App Engine Config (YAML)
Requires accessing the `app.yaml` file, updating it, then uploading back to GCS. Can be done the following way:

Download file to your local then decrypt it with:
```commandline
gsutil cp gs://cbh-member-service-prod-secrets/app-yaml-key/app.yaml.enc ~/temp/app.yaml.enc
gcloud kms decrypt --location=us --keyring cbh-member-service-prod --key app-yaml-key --ciphertext-file ~/temp/app.yaml.enc --plaintext-file ~/temp/app.yaml --project cbh-kms
```
Make updates using your text editor of choice (don't forget to save it!):
```commandline
open -a textedit ~/temp/app.yaml
```
Encrypt and upload back to GCS and remove from local
```commandline
gcloud kms encrypt --location=us --keyring cbh-member-service-prod --key app-yaml-key --ciphertext-file ~/temp/app.yaml.enc --plaintext-file ~/temp/app.yaml --project cbh-kms
gsutil cp ~/temp/app.yaml.enc gs://cbh-member-service-prod-secrets/app-yaml-key/app.yaml.enc
rm -f ~/temp/app.yaml.enc ~/temp/app.yaml
```
Redeploy the service to ensure configuration changes are applied (get tag from:
```commandline
TRIGGER_ID=b92dad8b-a677-4749-956e-ac6eff12271c
TAG_NAME=... # get latest tag name here: https://github.com/cityblock/mixer/tags
gcloud beta builds triggers run $TRIGGER_ID --tag $TAG_NAME --project cityblock-data
```
Alternatively the deployment step can be done on the Console UI

## What to do in case of failure
### Deployment failure
Investigate Cloud Build logs - there are 2 sets of them:
- `cityblock-data` contains logs related to the high level deployment process
- `cbh-member-service-prod` contains logs related to the low level (App Engine specific) deployment process 

## Appendix
- [Member Service README](../member_service/README.md)
