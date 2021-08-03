# Partner SFTP
Web server that allows partners to upload data which is then copied into Cityblock

## What is this service, and what does it do?
The SFTP application a set of pods on a GKE cluster that includes a large set of
 configurations that allow our partner to send us data. The disk on the server is
 namespaced according to the partner's name and all the files they've ever uploaded
 exist there in a flat structure (ex. all Emblem files are in `/sftp_homes/emblem/drop`).
 The pods are namely the SFTP application itself and a `gcloud` container that uses `gsutil`
 to synchronize all the files from the SFTP disk to a GCS bucket called `cbh_sftp_drop`. 
 We preserve the filesystem as much as possible in this bucket. It runs on a timer of
 calling the `gsutil rsync` command every 5 minutes, so the data should be available in a
 semi-realtime manner.

## Who is responsible for it?
Platform team

## What dependencies does it have?
GCP services: GCR, GCS, IAM, GKE, GCE (Persistent Disk)

## What does the infrastructure for it look like?
A Kubernetes deployment that exists as 2 pods as mentioned previously. 

There is also a Kubernetes service that exists as a load balancer to drop files off into the
 SFTP application which only allows a specific set of IP addresses that are documented in code.
 
Finally there are a large set of configurations that exist as both a secret (namely the service
 account for the sync of the files) and Kubernetes ConfigMaps that exist as keys that are used
 by the partners to use the application

## What metrics and logs does it emit, and what do they mean?
### Metrics
- [Log based metric for container error count](https://console.cloud.google.com/logs/query;query=resource.type%3D%22container%22%0Aseverity%3DERROR%0Aresource.labels.container_name%3D%22gsutil-sync%22%0AtextPayload:%2528%22exception%22%20OR%20%22error%22%20NOT%20%22ResumableUploadAbortException%22%2529?project=cityblock-data)

### Logs
- [Slack channel that gets a message when activity is done on the GCS bucket](https://cityblockhealth.slack.com/archives/CB4PYT2DD)
- [Logs for the SFTP deployment on Cloud Logging](https://console.cloud.google.com/logs/query;query=resource.type%3D%22container%22%0Aresource.labels.cluster_name%3D%22airflow%22%0Aresource.labels.namespace_id%3D%22default%22%0Aresource.labels.project_id%3D%22cityblock-data%22%0Aresource.labels.zone:%22us-east1-b%22%0Aresource.labels.container_name%3D%2528%22gsutil-sync%22%20OR%20%22sftp%22%2529%0Aresource.labels.pod_id%3D%22sftp-drop-5bd4757c9b-wrr2b%22?project=cityblock-data)

## What alerts are set up for it, and why?
### Disk capacity getting filled
The disk size needs to be increased, do the following to achieve this:
1. Edit the `PersistentVolumeClaim` size in `all.yml`. WARNING: size can only be increased, never decreased. 
1. Apply this change to the pod via `kubectl apply -f all.yml`
1. [Update the alerting policy](https://console.cloud.google.com/monitoring/alerting/policies/1325650137744658148?project=cityblock-data) to reflect this new capacity 
1. Create a PR with this change.
 

### SFTP container throwing errors/exceptions
The container is throwing exceptions/errors which are preventing the files from going to GCS
1. View the logs as mentioned to get details on what the error is
1. Only application logic exists in the [`all.yml`](../all.yml) - specifically in
 `spec.template.spec.containers.name[gsutil-sync].command`, use this as a means of debugging
 the source of the error
1. Make appropriate changes to resolve the error ([see an example of one in the past](https://github.com/cityblock/mixer/pull/417))
1. Ensure changes (if any) are applied and exceptions/errors are no longer thrown

## Tips from the authors and pros
- Visit logs on Kubernetes pods/cluster to get a sense of what occurred in the event of failures.

## Appendix
- [Application README](../README.md)
- [SFTP application source](https://github.com/cityblock/sftp)
