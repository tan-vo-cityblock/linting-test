module "cbh_secrets_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-secrets"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}
