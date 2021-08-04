resource "google_project_iam_member" "dbt_bq_data_editor" {
  project = module.analytics_staging_project.project_id
  member  = "serviceAccount:${module.dbt_staging_svc_acct_secret_data.email}"
  role    = "roles/bigquery.dataEditor"
}

resource "google_project_iam_member" "dbt_bq_job_user" {
  project = module.analytics_staging_project.project_id
  member  = "serviceAccount:${module.dbt_staging_svc_acct_secret_data.email}"
  role    = "roles/bigquery.jobUser"
}

resource "google_project_iam_member" "dbt_bq_user" {
  project = module.analytics_staging_project.project_id
  member  = "serviceAccount:${module.dbt_staging_svc_acct_secret_data.email}"
  role    = "roles/bigquery.user"
}

resource "google_project_iam_member" "data_team_editors" {
  project = module.analytics_staging_project.project_id
  member  = "group:data-team@cityblock.com"
  role    = "roles/editor"
}

resource "google_project_iam_member" "dbt_ref_data_bq_viewer" {
  member  = "serviceAccount:${module.dbt_staging_svc_acct_secret_data.email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.analytics_staging_project.project_id
  role       = "roles/bigquery.dataViewer"
  members    = ["group:actuary@cityblock.com"]
}
