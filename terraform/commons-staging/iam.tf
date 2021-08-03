resource "google_project_iam_member" "project_owner" {
  project = google_project.commons-staging.project_id
  member  = "group:eng-all@cityblock.com"
  role    = "roles/owner"
}

resource "google_project_iam_member" "segment-staging-bigquery-dataOwner" {
  project = google_project.commons-staging.project_id
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${google_service_account.segment-staging-svc-acct.email}"
}

resource "google_project_iam_member" "looker-bigquery-dataViewer" {
  project = google_project.commons-staging.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${module.looker_svc_acct.email}"
}

resource "google_project_iam_member" "eng-all-bigquery-dataViewer" {
  project = google_project.commons-staging.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "group:eng-all@cityblock.com"
}

resource "google_project_iam_member" "pmux-bigquery-dataViewer" {
  project = google_project.commons-staging.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "group:pmux@cityblock.com"
}

resource "google_project_iam_member" "data-team-bigquery-dataViewer" {
  project = google_project.commons-staging.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "group:data-team@cityblock.com"
}

resource "google_project_iam_member" "gcp_logger_logs_writer" {
  project = google_project.commons-staging.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${module.gcp_logger_svc_acct.email}"
}

resource "google_project_iam_member" "gcp_logger_error_writer" {
  project = google_project.commons-staging.project_id
  role    = "roles/errorreporting.writer"
  member  = "serviceAccount:${module.gcp_logger_svc_acct.email}"
}

// TODO: PLAT-1348 Create a storage.tf file and bucket module with IAM to match terraform/commons-production/storage.tf
resource "google_storage_bucket_iam_member" "staging_zendesk_worker_storage_object_admin" {
  member = "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}"
  role   = "roles/storage.objectAdmin"
  bucket = "cityblock-staging-patient-data"
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.commons-staging.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${google_service_account.segment-staging-svc-acct.email}",
    "serviceAccount:${google_service_account.commons-staging.email}"
  ]
}
