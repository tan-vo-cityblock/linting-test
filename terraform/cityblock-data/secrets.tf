data "google_secret_manager_secret_version" "dbt-staging-creds" {
  provider = google-beta
  project = "cbh-analytics-staging"
  secret = "dbt-run-staging"
  version = 1
}
