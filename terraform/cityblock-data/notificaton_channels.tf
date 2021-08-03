data "google_monitoring_notification_channel" "pagerduty" {
  project      = var.partner_project_production
  type         = "pagerduty"
  display_name = "Pagerduty"
}
