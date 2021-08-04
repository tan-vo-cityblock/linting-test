module "analytics_staging_project" {
  source     = "../src/resource/project"
  project_id = "cbh-analytics-staging"
  name       = "Analytics Staging"
  api_services = [
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "resourceviews.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "secretmanager.googleapis.com"
  ]
}
