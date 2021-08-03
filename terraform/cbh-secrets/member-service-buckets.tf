module "cbh_member_service_staging_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_staging_name}${local.secret_bucket_postfix}"
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

module "cbh_member_service_prod_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_prod_name}${local.secret_bucket_postfix}"
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
        "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}",
        "serviceAccount:${module.load_cci_service_account.email}",
        "serviceAccount:${module.load_emblem_service_account.email}",
        "serviceAccount:${module.load_emblem_pbm_service_account.email}",
        "serviceAccount:${module.load_tufts_daily_service_account.email}",
        "serviceAccount:${module.svc_acct_carefirst_worker.email}",
        "serviceAccount:${module.elation_worker_svc_acct.email}",
        "serviceAccount:${module.svc_acct_cardinal_worker.email}",
        "serviceAccount:${module.svc_acct_healthyblue_worker.email}",
        "group:data-team@cityblock.com",
        "serviceAccount:${module.prod_commons_mirror_svc_acct.email}",
        "serviceAccount:${module.staging_commons_mirror_svc_acct.email}",
        "serviceAccount:${module.prod_quality_measure_mirror_svc_acct.email}",
        "serviceAccount:${module.prod_member_index_mirror_svc_acct.email}",
        "serviceAccount:${module.staging_member_index_mirror_svc_acct.email}",
        "serviceAccount:${module.redox_worker_svc_acct.email}",
        "serviceAccount:${module.payer_suspect_svc_acct.email}"
      ]
    }
  ]
}
