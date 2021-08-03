module "quality_measure_service_staging_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.qm_service_staging_app_yaml_key.ring_name}${local.secret_bucket_postfix}"
  project_id = module.cbh_secrets_project.project_id

  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/storage.objectViewer"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
        "serviceAccount:${module.staging_cityblock_data_project_ref.default_compute_engine_service_account_email}"
      ]
    }
  ]
}

module "quality_measure_service_prod_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.qm_service_prod_app_yaml_key.ring_name}${local.secret_bucket_postfix}"
  project_id = module.cbh_secrets_project.project_id

  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/storage.objectViewer"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
        "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}"
      ]
    }
  ]
}
