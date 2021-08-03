resource "google_cloudbuild_trigger" "deploy_sftp_drop_trigger" {
  description = "Redeploy sftp_drop_trigger GCF"
  name        = "deploy-sftp-drop-trigger"
  trigger_template {
    project_id  = module.gcp_project_cityblock_data.project_id
    repo_name   = "github_cityblock_mixer"
    branch_name = "^master$"
  }
  project = module.gcp_project_cityblock_data.project_id

  substitutions = {
    _FXN_NAME   = google_cloudfunctions_function.sftp_drop_trigger.name
    _FXN_SOURCE = google_cloudfunctions_function.sftp_drop_trigger.source_repository.0.url
    _FXN_REGION = google_cloudfunctions_function.sftp_drop_trigger.region
  }
  included_files = [
    "cloud_functions/sftp_drop_trigger/**"
  ]
  filename = "cloud_functions/cloudbuild.yaml"
}

resource "google_service_account_iam_member" "default_cloudbuild_service_account_uses_sftp_drop_trigger_service_account" {
  service_account_id = module.service_account_sftp_drop_trigger.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:97093835752@cloudbuild.gserviceaccount.com" # hardcoded because data "google_service_account" regex doesn't handle this
}
