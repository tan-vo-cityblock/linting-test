resource "google_secret_manager_secret" "dbt_staging_svc_acct" {
  provider  = google-beta
  project   = module.analytics_staging_project.project_id
  secret_id = "dbt-run-staging"
  replication {
    user_managed {
      replicas {
        location = "us-east4"
      }
    }
  }
}

module "dbt_staging_svc_acct_secret_data" {
  source              = "../src/custom/svc_acct_key_secret_version"
  project_id          = module.analytics_staging_project.project_id
  secret_id           = google_secret_manager_secret.dbt_staging_svc_acct.id
  account_id          = "dbt-run-staging"
  account_description = "Service Account used for dbt staging runs"
}

resource "google_secret_manager_secret_iam_member" "cloudbuild_dbt_staging_access" {
  provider  = google-beta
  project   = module.analytics_staging_project.project_id
  secret_id = google_secret_manager_secret.dbt_staging_svc_acct.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.cityblock_data_reference.default_cloudbuild_service_account_email}"
}
