// Used to get the state for the root 'main.tf' in the /terraform directory
data "terraform_remote_state" "cbh_root_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/state"
  }
}
