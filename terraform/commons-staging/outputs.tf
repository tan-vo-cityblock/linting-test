output "commons_staging_svc_acct_email" {
  value       = google_service_account.commons-staging.email
  description = "Service account associated with Commons for all runs on staging"
}
