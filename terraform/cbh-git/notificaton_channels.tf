data "google_monitoring_notification_channel" "pagerduty" {
  project      = module.cityblock_data_project_ref.project_id # all notifications channels are in cityblock-data workspace
  type         = "pagerduty"
  display_name = "Pagerduty"
}
