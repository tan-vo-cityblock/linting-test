module "commons_github_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.commons_github_creds.ring_name}-secrets"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}