module "cbh_db_mirror_member_index_staging" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.cbh_db_mirror_staging.ring_name}-secrets"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}

module "cbh_db_mirror_member_index_prod" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.cbh_db_mirror_prod.ring_name}-secrets"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}
