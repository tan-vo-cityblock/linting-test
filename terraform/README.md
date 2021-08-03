# Terraform

Terraform repo for our GCP "Infrastructure as Code" initiative.

* [Getting Started](#getting-started)
* [Helpful Links](#helpful-links)
* [Pull Request Process](#pull-request-process)
* [Resource Provisioning](#resource-provisioning)
* [Secret Management](#secret-management)
* [Cloud KMS **DEPRECATED**](#cloud-kms-deprecated)
* [Random Tidbits](#random-tidbits)
* [Personal Projects](#personal-projects)
* [Cityblock Data](#cityblock-data)
* [Persisted Views](#persisted-views)
* [Database Mirroring](#database-mirroring)

## Getting Started

1. [Install Terraform](https://learn.hashicorp.com/terraform/getting-started/install.html) for your platform (put it in your PATH)
1. [Install `kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-with-homebrew-on-macos) (_Kube Control_)
1. Configure your `kubeconfig` entry(ies) by running the [relvant GKE auth scripts](/scripts/auth)
1. `cd` to this directory and run `terraform init` to download modules locally
1. Ensure you have `terraform.tfvars` file available in `mixer/terraform/` path. [Copy it from OneLogin secure notes here](https://cityblock.onelogin.com/notes/127520).
1. Make your changes, or run `terraform refresh` to ensure things are good
1. Run `terraform plan` to see the changes that would be made
1. To apply the changes to infrastructure, run `terraform apply`

## Helpful Links
- [Google Cloud Terraform Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

## Pull Request Process

Only GCP Admins should merge terraform pull requests. Currently this isn't enforced in GitHub, but we will try to
do that soon. In the meantime, we apologize for the inconvenience.

This is because GCP Admins are the only people with `terraform apply` permissions, and we don't want any change
getting merged into master without getting applied first. Otherwise, subsequent runs of `terraform apply` for a new pull
request will include changes from older pull requests.

## Resource provisioning
- When creating a new resource, first [check in the `src` directory](./src) to confirm one exists for the
 resource you want to create (like a BQ dataset). Prefer to use that over the default one available in the
 Google provider. This abstraction is done to ensure consistency in our Terraform setup.
- Utilize the `labels` attribute accordingly, [consult the Cloud Assets guide maintained 
 by the Platform team.](https://docs.google.com/document/d/1XEaGoLcpkDokL7P5MAsWs_wywfCfiRusSYyhY1PvYoA/edit#heading=h.1iqrv2fwdmjq)

## Secret Management

For security, we want to eliminate direct sharing of secrets between engineers in ad-hoc ways and create a secure / auditable
source of truth for our secret data.

The GCP offering, [Secret Manager](https://cloud.google.com/secret-manager/docs/overview#top_of_page), allows us to store, manage,
and access secrets as binary blobs or text strings.

With appropriate permissions, you can view the contents of the secret and update / rotate the secret.

* [Overview](#overview)
* [List Secrets](#list-secrets)
* [Pull Secrets](#pull-secrets)
* [Create Secrets](#create-secrets)
    * [Create a Secret Manager Secret in Terraform](#create-a-secret-manager-secret-in-terraform)
    * [Load your secret data in to the Secret resource as a Secret Version.](#load-your-secret-data-in-to-the-secret-resource-as-a-secret-version)
    * [[Optional] Load your Secret Version into a Kubernetes cluster.](#optional-load-your-secret-version-into-a-kubernetes-cluster)
* [Update Secret Versions](#update-secret-versions)

### Overview

A GCP project, [cbh-secrets](https://console.cloud.google.com/security/secret-manager?folder=&organizationId=&project=cbh-secrets), exists
specifically for secret management to ensure separation of duties between the storage of secrets and users of secrets.

The project exclusively holds our Secret Manager resource where secrets are stored for access.

We've abstracted common [gcloud secret ..](https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets)
commands into scripts defined in [mixer/scripts/secret_manager](../scripts/secret_manager) for ease of use.

- **The below commands assume that you first run:**

  `cd ~/mixer/scripts/secret_manager`

### List Secrets

- To generate a list of all Secret resources stored in Secret Manager, run:

  `bash list_secrets.sh`

- To print a list of all Secret Versions created in a Secret resource, run:

  `bash list_secrets.sh [secret_id]`
  
- **Note**: If you don't see a secret listed here, it may still live in Cloud KMS which is deprecated.
  
  See [Cloud KMS **DEPRECATED** instructions ](#cloud-kms-deprecated) below to pull down the secret
  then please migrate to Secret Manager per instructions here.

### Pull Secrets

- When we access secrets, it is best practice to save the secret to a file instead
  of printing to the console to avoid storing the secret in your shell history.

- To access a secret and store locally run:

  `bash pull_secret.sh [secret_id] [path/to/save/secret_file.ext]`

- `secret_id` will be the id of a secret returned when you list secrets.

- `path/to/save/secret_file` should be a path to a non-git directory.

  - You can use hints in the `secret_id` value or inspect the `secret_file` to add
    appropriate file extensions (i.e. .json, .conf) as needed.

- For accessing secrets in application code, see [GCP Client Library docs](https://cloud.google.com/secret-manager/docs/reference/libraries).

### Create Secrets

- We want to be as [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) as possible with secrets
  so we only have to update / rotate them in one place.

- Before creating a new secret, list and inspect existing secrets per instructions above, to confirm
  the secret value isn't already stored.

- Even in the case where you need a subset of secrets stored in an existing secret, i.e. an `API_KEY` stored in
  a `cool_service_app_yaml` secret, aim to pull the existing secret and parse it in your application code to
  grab the specific secret you need.

- Creating secrets is a 2 (optionally 3) step workflow:

  1. Create the Secret Manager Secret GCP resource via Terraform.
  1. Manually load secret data into the Secret resource thus creating a Secret Version.
  1. [Optional] Load the secret data into a Kubernetes cluster.

#### Create a Secret Manager Secret in Terraform

- As mentioned above, all secrets live in GCP project `cbh-secrets` which corresponds to the
  Terraform state defined in [mixer/terraform/cbh-secrets](./cbh-secrets).

- Create and edit a `/cbh-secrets/logically-named-secret.tf` file using [zendesk-secrets-staging.tf](./cbh-secrets/zendesk-secrets-staging.tf) as a guide.

- Add your secret module in the file:

  ```
  module "[logically_named_json_secret]" {
    source    = "../src/resource/secret_manager/secret"
    secret_id = "[logically_named_json]"
    secret_accessors = [
      "serviceAccount:${module.cool_svc_acct.email}",
    ]
  }
  ```
  - Note that `secret_id` can contain uppercase and lowercase letters, numerals, and the hyphen ( - ) and underscore
   ( _ ) characters.

  - If you know the extension of the secret data file, try to include it in the value of `secret_id = `.
    It is also fine if your secret is a plaintext file with no extension.

  - JSON file type is a good default as they are easy to parse, and the key-value format helps when grouping related secrets.

  - By default, `group:eng-all@cityblock.com` can access all secrets, so only append service accounts / users
    outside of eng-all to the list `secret_accessors` or leave empty.

- Submit a PR with your changes for approval, then [apply your](https://www.terraform.io/docs/commands/apply.html)
  Terraform module to create the secret in GCP.

- Confirm your Secret resource was created by running `bash list_secrets.sh` per above instructions.

#### Load your secret data in to the Secret resource as a Secret Version.

- A Secret Version contains the actual secret data, along with state and metadata about the secret.

- For now, only members of `group:gcp-admins@cityblock.com` have the permission to add Secret Versions.

- Add your secret data as a Secret Version to the Secret resource you created in the previous step:

  `bash create_secret_version.sh [secret_id] [path/to/local/secret_file]`

- Note that there is a 1-to-1 relationship between a Secret resource and the secret data added as a
  Secret Version.

  - This means we can add a new Secret Version when the secret data is updated, i.e. a password is reset.

  - However, do not use Secret Versions as a way to store entirely different secrets in a Secret resource.

- Each call to this script will automatically tag the most recently added Secret Version
  with `latest` for the given `secret_id`, and the `latest` Secret Version will be returned when pulling secrets.

- Confirm your Secret Version was added by running `bash list_secrets.sh [secret_id]`

#### [Optional] Load your Secret Version into a Kubernetes cluster.

- You may need to make your secret data available in a Kubernetes cluster for use in Airflow.

- You can do so after you've added the Secret Version per the previous step.

- Append the below module to your `/cbh-secrets/logically-named-secret.tf` file:

  ```
  module "[logically_named_json_secret]_k8s_secret" {
    providers = {
      kubernetes = "kubernetes.cityblock-orchestration-[env]"
    }
    source         = "../src/custom/secret_manager_secret_to_k8_secret"
    secret_id      = module.[logically_named_json_secret].secret_id
    project_number = module.cbh_secrets_project.project_number
  }
  ```
- Note that you can replace `[env]` with `test` or `prod` depending on the cluster you want to load the secret into.

- Submit your changes in a PR for approval and apply them.

- Confirm your secret exists in the cluster by running:

  ```
  kubectl config use-context $[PROD_CLUSTER OR TEST_CLUSTER]
  kubectl describe secret [secret_id]
  ```
  - See [kubectl authentication scripts](../scripts/auth) if you experience auth errors.

### Update Secret Versions

- Read the GCP [docs on managing Secret Versions](https://cloud.google.com/secret-manager/docs/managing-secret-versions)
  to carry out the recommended workflow for updating Secret Versions.

- If a Secret Version is updated and the secret data was previously loaded into a Kubernetes cluster per instructions above,
  you will need to re-apply the Terraform module to also update the secret in Kubernetes:

  ```
  cd ~/mixer/terraform/cbh-secrets
  terraform apply -target=module.[logically_named_json_secret]_k8s_secret
  ```

- For now, only members of `group:gcp-admins@cityblock.com` have the permission to update Secret Versions.


## Cloud KMS **DEPRECATED**

**See [Secret Management](#secret-management)**

We have implemented a process using [GCP's Cloud KMS](https://cloud.google.com/kms/docs/) for more secure
secret management and the below steps can be carried out by members of the GCP Admins group (gcp-admins@cityblock.com)

   * [Overview](#overview)
   * [A. Cloud KMS Key Rings and Keys](#a-cloud-kms-key-rings-and-keys)
       * [CLI - Decryption](#cli---decryption)
   * [B. Encrypted Secret Storage - GCS](#b-encrypted-secret-storage---gcs)
        * [CLI - Deleting Encrypted Secrets](#cli---deleting-encrypted-secrets)


#### Overview

Two GCP projects exist specifically for secret management to ensure separation of duties between individuals and
services who *use* secrets versus those who *manage* secrets.

The first project `cbh-kms` exclusively holds cryptographic keys used for encrypting and decrypting of secrets.
The second project `cbh-secrets` exclusively holds GCS buckets where encrypted secrets are stored for access.

#### A. Cloud KMS Key Rings and Keys

A key ring is a grouping of keys for organizational purposes. A key ring belongs and is namespaced to a GCP Project
(`cbh-kms`) and resides in a specific location (`"us"`). A key belongs to, is namespaced by, and inherits permissions
from it's key ring.

Grouping keys with related permissions together in a key ring allows you to grant, revoke, or modify
permissions to those keys at the key ring level, without needing to act on each key individually.

Once created, key rings and keys cannot be deleted so pay attention to naming conventions and spelling.

Name key rings to match GCP project ids (i.e. cbh-member-service-prod), or logical entities that have secrets
(i.e. cbh-elation-prod, cbh-dbt-prod), with __GCP project ids taking priority__. Name keys to match the file type they are
encrypting (i.e. app-yaml, db-password).

Note that we want key ring names to match GCP project ids to facilitate interpolation when accessing key rings
and their secrets. See the example below on naming practices:


| Project                 | Entity         | Key Ring                | Secret File | Key          |
|:-----------------------:|:--------------:|:-----------------------:|:-----------:|:------------:|
| cbh-member-service-prod | Member Service | cbh-member-service-prod | app.yaml    | app-yaml     |
| n/a                     | Elation        | cbh-elation-prod        | my.cnf      | mysql-config |


Note: we base64 encode the encrypted secret b/c the terraform
[google provider REST API](https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/decrypt)
expects kms encrypted files to be base64 encoded.

##### CLI - Decryption
* For testing purposes, you can decrypt using the below bash commands and args which call the
[cbh_decrypt script](../scripts/kms/cbh_decrypt).
* Note: You should try to avoid decrypting real secrets to your local machine and restrict decrypting to the application
level.

```bash
cd mixer/scripts/kms
bash cbh_decrypt RING_NAME KEY_NAME PATH/TO/SECRET.64.enc PATH/TO/SECRET
```

* Download and decrypt the secret directly from GCS by running the below script (you can also provide a 4th argument to
specify the filename in case it is different from the secret name, must also exclude the `.64.enc`):
```bash
cd mixer/scripts/kms
bash cbh_decrypt_from_gcs RING_NAME KEY_NAME PATH/TO/SAVE/SECRET/FILE (filename must be the secret name in gcs without extension '.64.enc')
```
#### B. Encrypted Secret Storage - GCS

To store encrypted secrets, we create gcs buckets with a URI namespaced on the key ring and key as such:
`gs://[RING_NAME]-secrets/[KEY_NAME]/[SECRET].64.enc`. We append `-secrets` to the bucket name to avoid bucket naming
conflicts that could occur when `RING_NAME` equals GCP `project-id` and the project could have a bucket named after it's
`project-id`.

##### CLI - Deleting Encrypted Secrets
* If an encrypted secret is no longer used / needed, it can be deleted from GCS by running:
```bash
gsutil rm gs://[RING_NAME]-secrets/[KEY_NAME]/[SECRET].64.enc
```
* Note: confirm whether the key needs to be [disabled](https://cloud.google.com/kms/docs/key-rotation#manual_rotation)
or [destroyed](https://cloud.google.com/kms/docs/destroy-restore) as part of secret deletion.


## Random Tidbits

- State is stored on GCS, and only gcp-admins@cityblock.com has access to it. This provides locking and sharing of state across the org.

## Personal Projects

If you need a sandbox project, you can create one by following the directions in [personal-projects/README.md](personal-projects/README.md)

## Cityblock Data
If you need to delegate a service account to perform events in the cityblock-data project in GCP, follow directions in [cityblock-data/README.md](cityblock-data/README.md).

## Persisted Views

If you need a view persisted to BigQuery, use [DBT](../dbt/README.md).

## Database Mirroring

If you would like to mirror a database from Cloud SQL to BigQuery, you will need to configure a few Terraform components:
 * Navigate to `cbh-db-mirror-{env}` where env specifies the environment of the Cloud SQL instance you are mirroring
   from. Find the `mirror-jobs.tf` file in this directory and add [a custom Cloud SQL to BigQuery module](src/custom/cloudsql_export_to_bq/main.tf)
   specifying the cloud sql database you would like to mirror. Most parameters should be consistent across all modules within an
   environment (`source`, `project_id`, `environment`, `cloud_sql_instance_svc_acct`). A couple will need
   to be customized for your mirroring job (`gcs_bucket_name`, `bq_dataset_id`, `database_name`).

 * The other Terraform piece centers around the credentialing for the new mirror job:
    * (Note that this step is not necessary if credentials already exist for mirroring from this Cloud SQL instance. For all
      of the following steps, you may use the code creating the `member-index` key rings as an example.)

      Within `cbh-kms`, navigate to [`locals.tf`](cbh-kms/locals.tf). Add to the locals spec a user and password for the
      instance being mirrored from. Then, go to `cbh-db-mirror-{env}-kms.tf` where again env specifies the environment of
      the Cloud SQL instance your are mirroring from. In this file, add two modules that will house the user and password
      for accessing the instance. Finally, head to [`outputs.tf`](cbh-kms/outputs.tf) and create two outputs for the user
      and password key ring modules that you have just created.

    * (Note that this step is not necessary if the bucket storing the key ring already exists for mirroring from this Cloud
      SQL instance. If it does not, you can use the bucket creation code for mirroring the `member-index` as an example.)

      Within `cbh-secrets`, navigate to `cbh-db-mirror-buckets.tf`. Add another module here specifying the GCS bucket
      that the key ring will be stored in. The name of the bucket can be obtained from the `ring_name` on the KMS output
      for user of the mirrored instance.

    * (Note that this step is not necessary if the k8 secrets have already been added for the instance being mirrored from.
      If they do not, you can use the k8 secret creation code for mirroring the `member-index` as an example.)

      Within `cityblock-orchestration`, navigate to `k8_secrets_cbh_db_mirror_{env}.tf`. Add two local values specifying
      the user and password taken from the corresponding KMS output. Then for both the user and password, create a [storage object](src/custom/storage_object/main.tf)
      module pointing to the encoded secret as well as its corresponding data object. Finally, create a [kubernetes](src/resource/kubernetes/main.tf)
      resource using the data objects provisioned above.
