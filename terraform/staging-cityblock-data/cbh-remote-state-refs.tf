// Defined once here and used by multiple -object-acl.tf to grab key ring & key names to generate gcs URI to ciphertext object
// so READ IAM can be set on project's default service accounts.

data "terraform_remote_state" "cbh_kms_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/kms"
  }
}

data "terraform_remote_state" "cbh_services_staging_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-services-staging"
  }
}
