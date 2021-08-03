resource "google_project_iam_member" "project_owner" {
  project = google_project.commons-production.project_id
  member  = "group:eng-all@cityblock.com"
  role    = "roles/owner"
}

resource "google_project_iam_member" "segment-production-bigquery-dataOwner" {
  project = google_project.commons-production.project_id
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${module.segment_production_svc_acct.email}"
}

resource "google_project_iam_member" "gcp_logger_logs_writer" {
  project = google_project.commons-production.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${module.gcp_logger_svc_acct.email}"
}

resource "google_project_iam_member" "gcp_logger_error_writer" {
  project = google_project.commons-production.project_id
  role    = "roles/errorreporting.writer"
  member  = "serviceAccount:${module.gcp_logger_svc_acct.email}"
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.commons-production.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:data-team@cityblock.com",
    "group:pmux@cityblock.com",
    "serviceAccount:${module.looker_svc_acct.email}",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}"
  ]
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.commons-production.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${google_service_account.commons-production.email}",
    "serviceAccount:${module.segment_production_svc_acct.email}"
  ]
}

module "error_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.commons-production.project_id
  role       = "roles/errorreporting.viewer"
  members = [
    "group:data-team@cityblock.com"
  ]
}
