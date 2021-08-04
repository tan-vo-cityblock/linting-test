resource "google_cloudfunctions_function" "sftp_drop_trigger" {
  project               = module.gcp_project_cityblock_data.project_id
  name                  = "sftp_drop_trigger"
  description           = "Respond to all new objects in sftp_drop GCS bucket."
  runtime               = "python37"
  region                = "us-east4"
  available_memory_mb   = 512
  timeout               = 60
  entry_point           = "cf"
  service_account_email = module.service_account_sftp_drop_trigger.email
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "cbh_sftp_drop"
  }
  source_repository {
    url = "https://source.developers.google.com/${var.sourcerepo_id}/moveable-aliases/master/paths/cloud_functions/sftp_drop_trigger"
  }
}


