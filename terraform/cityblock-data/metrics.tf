resource "google_logging_metric" "sftp_errors" {
  project     = var.partner_project_production
  name        = "sftp-container-errors"
  description = "Errors logged on the SFTP sync to GCS container"
  filter      = <<EOT
resource.type="container"
severity=ERROR
resource.labels.container_name="gsutil-sync"
textPayload:("exception" OR "error" NOT "ResumableUploadAbortException")
  EOT
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}
