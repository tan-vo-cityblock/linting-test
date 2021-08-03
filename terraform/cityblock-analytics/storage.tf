module "analytics_config_bucket" {
  source        = "../src/resource/storage/bucket"
  name          = "cbh-analytics-configs"
  project_id    = var.project_id
  location      = "US-EAST4"
  storage_class = "STANDARD"
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com",
        "group:data-team@cityblock.com",
        "serviceAccount:${module.dnah_jobs_svc_acct.email}"
      ]
    },
    {
      role = "roles/storage.objectViewer"
      members = [
        "serviceAccount:${module.slack_platform_bot_svc_acct.email}"
      ]
    },
    {
      role = "roles/storage.legacyBucketOwner"
      members = [
        "projectEditor:${module.cbh_analytics_project.project_id}",
        "projectOwner:${module.cbh_analytics_project.project_id}"
      ]
    },
    {
      role = "roles/storage.legacyBucketReader"
      members = [
        "projectViewer:${module.cbh_analytics_project.project_id}",
        "serviceAccount:${module.slack_platform_bot_svc_acct.email}"
      ]
    }
  ]
}

module "analytics_artifacts_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-analytics-artifacts"
  project_id = var.project_id
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com",
        "group:data-team@cityblock.com",
        "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
        "serviceAccount:${module.svc_acct_ge.email}",
        "serviceAccount:${module.tf_svc_load_monthly_cci_ref.email}",
        "serviceAccount:${module.load_tufts_service_account.email}",
        "serviceAccount:${module.load_emblem_service_account.email}",
        "serviceAccount:${module.cbh_analytics_project.default_app_engine_service_account_email}"
      ]
    }
  ]
}
