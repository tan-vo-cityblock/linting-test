resource "google_cloud_scheduler_job" "refresh_hie_message_signal_view_staging" {
  name             = "refresh-hie-message-signal-view-staging"
  description      = "Refreshes the HIE Message Signal materialized view"
  project          = "cbh-services-staging"
  # Every 10 minutes
  schedule         = "*/10 * * * *"
  time_zone        = "America/New_York"
  region           = "us-east4"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://health-events-service-staging-7a4htmb5sq-uk.a.run.app/schedule/refresh-hie-message-signal-view"
  }
}