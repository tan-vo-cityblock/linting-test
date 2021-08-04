resource "google_logging_metric" "gitlab_backup_log" {
  project     = module.git_project.project_id
  name        = "gitlab-backup-to-gcs"
  description = "API calls made to sync GitLab backup from instance to GCS"
  filter      = "protoPayload.requestMetadata.callerSuppliedUserAgent=\"fog-core/2.1.0,gzip(gfe)\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "bucket_name"
      value_type = "STRING"
    }
  }
  label_extractors = {
    "bucket_name" = "EXTRACT(resource.labels.bucket_name)"
  }
}
