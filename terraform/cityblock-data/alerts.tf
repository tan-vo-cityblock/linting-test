resource "google_monitoring_alert_policy" "sftp_errors" {
  project      = var.partner_project_production
  combiner     = "OR"
  display_name = "SFTP Sync to GCS has errors"
  conditions {
    display_name = "Count of application errors (exceptions) reported in logs"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      filter          = "metric.type=\"logging.googleapis.com/user/sftp-container-errors\" resource.type=\"gke_container\""
      duration        = "7200s" # 2 hours
      threshold_value = 22.0  # there are 24 five minute intervals in 2 hours, so a couple less is enough to pass threshold
      aggregations {
        alignment_period = "7200s"
        per_series_aligner = "ALIGN_DELTA"
      }
      trigger {
        count = 1
      }
    }
  }
  documentation {
    content   = <<EOT
Errors found in SFTP sync operation for the past 2 hours - please look into it
[Playbook for the SFTP sync operation error](https://github.com/cityblock/mixer/tree/master/sftp/playbooks/sftp.md#sftp-container-throwing-errorsexceptions)
    EOT
    mime_type = "text/markdown"
  }
  notification_channels = [data.google_monitoring_notification_channel.pagerduty.name]
}
