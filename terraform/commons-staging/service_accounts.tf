# Data references
module "looker_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-data"
  account_id = "looker-demo"
}

# Resource creations
resource "google_service_account" "commons-staging" {
  account_id   = "commons-staging"
  display_name = "commons-staging"
  project      = google_project.commons-staging.project_id
}

resource "google_service_account" "segment-staging-svc-acct" {
  account_id   = "segment-staging"
  display_name = "segment-staging"
  project      = google_project.commons-staging.project_id
}

module "gcp_logger_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = google_project.commons-staging.project_id
  account_id = "gcp-logger"
}

module "staging_zendesk_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "staging-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}
