resource "google_cloud_scheduler_job" "refresh_hie_message_signal_view_prod" {
  name             = "refresh-hie-message-signal-view-prod"
  description      = "Refreshes the HIE Message Signal materialized view"
  project          = "cbh-services-prod"
  # Every 10 minutes
  schedule         = "*/10 * * * *"
  time_zone        = "America/New_York"
  region           = "us-east4"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://health-events-service-prod-bz54ntlpoq-ue.a.run.app/schedule/refresh-hie-message-signal-view"
  }
}