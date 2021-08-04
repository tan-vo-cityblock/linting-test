resource "google_project_iam_member" "project_owner" {
  project    = var.project_staging
  member     = "group:eng-all@cityblock.com"
  role       = "roles/owner"
}

resource "google_pubsub_topic_iam_member" "computed_fields_publisher_data_team" {
  topic   = "newComputedFieldResults"
  project = var.project_staging
  role    = "roles/editor"
  member  = "group:data-team@cityblock.com"
}

// TODO: PLAT-1348 Consolidate project level IAM into project_iam_access modules
module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = var.project_staging
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}",
  ]
}
