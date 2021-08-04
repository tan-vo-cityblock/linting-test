resource "google_project_iam_member" "cloudbuild_svc_acct_cloudfxn_dev" {
  project = module.gcp_project_cityblock_data.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${module.service_account_sftp_drop_trigger.email}"
}

