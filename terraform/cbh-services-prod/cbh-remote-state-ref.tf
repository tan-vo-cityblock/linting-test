data "terraform_remote_state" "cbh_secrets_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-secrets"
  }
}
