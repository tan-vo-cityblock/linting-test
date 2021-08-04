module "looker_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-data"
  account_id = "looker-demo"
}

module "dbt_staging_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cbh-analytics-staging"
  account_id = "dbt-run-staging"
}

module "dbt_prod_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-analytics"
  account_id = "dbt-prod"
}

resource "google_service_account" "commons-production" {
  account_id   = "commons-production"
  display_name = "commons-production"
  project      = google_project.commons-production.project_id
}

module "segment_production_svc_acct" {
  source     = "../src/resource/service_account"
  account_id = "segment-production"
  project_id = google_project.commons-production.project_id
}

module "gcp_logger_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = google_project.commons-production.project_id
  account_id = "gcp-logger"
}

module "elation_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "elation-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "dataflow_job_runner_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "dataflow-job-runner"
  project_id = "cityblock-data"
}

module "patient_eligibilities_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "patient-eligibilities"
  project_id = "cityblock-orchestration"
}

module "panel_management_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "tf-svc-panel-management"
  project_id = "cityblock-orchestration"
}

module "redox_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "redox-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "prod_zendesk_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}
