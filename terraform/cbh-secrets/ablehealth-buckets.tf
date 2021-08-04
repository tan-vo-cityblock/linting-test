module "cbh_ablehealth_prod" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.ablehealth_user_prod.ring_name}-secrets"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}
