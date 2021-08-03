module "cbh_dbt_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.dbt_ring_prod_name}${local.secret_bucket_postfix}"
  project_id = module.cbh_secrets_project.project_id
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role    = "roles/storage.objectViewer"
      members = ["serviceAccount:${module.cityblock_orchestration_project_ref.default_cloudbuild_service_account_email}"]
    }
  ]
}
