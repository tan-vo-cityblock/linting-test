// Defined once here and used by multiple .tf files to grab key ring & key names to generate gcs URI to ciphertext object
data "terraform_remote_state" "cbh_kms_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/kms"
  }
}

// used to get the state for the root 'main.tf' in the /terraform directory
data "terraform_remote_state" "cbh_root_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/state"
  }
}

// used to get the state for /cbh-services-staging directory
data "terraform_remote_state" "cbh_services_staging_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-services-staging"
  }
}


