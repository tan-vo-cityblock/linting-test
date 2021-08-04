module "dbt_staging_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cbh-analytics-staging"
  account_id = "dbt-run-staging"
}

module "sftp_sync_svc_acct" {
  source     = "../src/data/service_account"
  project_id = var.partner_project_production
  account_id = "sftp-gcs-sync"
}

module "sftp_backup_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_cold_storage_project_ref.project_id
  account_id = "cbh-sftp-drop-backup"
}

module "dnah_jobs_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-orchestration"
  account_id = "dnah-job-runner"
}

module "commons_dev_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-development"
  account_id = "cityblock-development"
}

module "load_tufts_daily_service_account" {
  source     = "../src/data/service_account"
  project_id = "cityblock-orchestration"
  account_id = "load-daily-tufts"
}

module "svc_acct_carefirst_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "carefirst-worker"
}

module "svc_acct_cardinal_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "cardinal-worker"
}

module "svc_acct_healthyblue_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "healthyblue-worker"
}

module "dbt_prod_svc_acct" {
  source = "../src/data/service_account"
  project_id = "cityblock-analytics"
  account_id = "dbt-prod"
}

module "sli_metrics_service_account" {
  source     = "../src/data/service_account"
  project_id = "cbh-sre"
  account_id = "sli-metrics"
}

# resource creation
resource "google_service_account" "elation_hook_svc_acct" {
  project      = "cityblock-data"
  account_id   = "tf-svc-elation-hook-access"
  display_name = "tf-svc-elation-hook-access"
}

module "airflow_app_engine_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = var.partner_project_production
  account_id = "airflow-app-engine-firewall"
}

module "slack_platform_bot_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = var.partner_project_production
  account_id = "slack-platform-bot"
}

module "prod_cureatr_worker_svc_acct" {
  source     = "../src/resource/service_account"
  account_id = "prod-cureatr-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "dev_cureatr_worker_svc_acct" {
  source     = "../src/resource/service_account"
  account_id = "dev-cureatr-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "prod_healthgorilla_worker_svc_acct" {
  source     = "../src/resource/service_account"
  account_id = "prod-healthgorilla-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "dev_healthgorilla_worker_svc_acct" {
  source     = "../src/resource/service_account"
  account_id = "dev-healthgorilla-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "pubsub_bq_saver_cf_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = var.partner_project_production
  account_id = "pubsub-bq-saver-cf"
}

module "elation_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "elation-worker"
  project_id =  module.cityblock_orchestration_project_ref.project_id
}

module "redox_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "redox-worker"
  project_id =  module.cityblock_orchestration_project_ref.project_id
}

module "ml_labeling_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cbh_ds_ref.project_id
  account_id = "ml-labeling"
}

module "datadog_metrics_svc_acct" {
  source = "../src/resource/service_account"
  project_id = var.partner_project_production
  account_id = "datadog-metrics-forwarding"
}

module "prod_zendesk_worker_svc_acct" {
  source = "../src/data/service_account"
  account_id = "prod-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "staging_zendesk_worker_svc_acct" {
  source = "../src/data/service_account"
  account_id = "staging-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "development_dataflow_svc_acct" {
  source = "../src/data/service_account"
  account_id = "dataflow"
  project_id = module.cbh_dev_ref.project_id
}

module "able_health_worker_svc_acct" {
  source = "../src/data/service_account"
  account_id = "tf-svc-able-health"
  project_id = module.cityblock_orchestration_project_ref.project_id
}
