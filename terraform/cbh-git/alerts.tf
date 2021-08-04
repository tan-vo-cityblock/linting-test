resource "google_monitoring_alert_policy" "gitlab_backups_stale" {
  project      = module.cityblock_data_project_ref.project_id # all alert policies are in cityblock-data workspace
  combiner     = "OR"
  display_name = "GitLab Backups Stale"
  conditions {
    display_name = "Count of backup sync to GCS"
    condition_threshold {
      comparison      = "COMPARISON_LT"
      filter          = <<EOT
metric.type="logging.googleapis.com/user/${google_logging_metric.gitlab_backup_log.id}"
AND resource.type="gcs_bucket" AND resource.label."bucket_name"="${module.gitlab-ee.omnibus_backup_bucket}"
      EOT
      duration        = "21600s" # 6 hours
      threshold_value = 1.0
      trigger {
        count = 1
      }
    }
  }
  documentation {
    content   = <<EOT
GitLab backups are stale (no backups in past 12 hours)

[GitLab Infrastructure reference](https://docs.google.com/document/d/1Ty84Iyqpab8Tm6eQALFNzpxVaykVFonqWEQxS4T4nhA/edit#heading=h.vpmz6o8nvwiv)
    EOT
    mime_type = "text/markdown"
  }
  notification_channels = [data.google_monitoring_notification_channel.pagerduty.name]
}
