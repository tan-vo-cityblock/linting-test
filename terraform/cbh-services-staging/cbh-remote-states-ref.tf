data "terraform_remote_state" "cbh_shared_vpc_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-shared-vpc-staging"
  }
}

data "terraform_remote_state" "cbh_secrets_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-secrets"
  }
}
