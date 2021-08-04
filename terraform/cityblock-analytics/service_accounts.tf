module "dbt_prod_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = module.cbh_analytics_project.project_id
  account_id = "dbt-prod"
}

module "dbt_staging_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cbh_analytics_staging_project_ref.project_id
  account_id = "dbt-run-staging"
}

module "dnah_jobs_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "dnah-job-runner"
}

module "slack_platform_bot_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_data_project_ref.project_id
  account_id = "slack-platform-bot"
}

module "sli_metrics_service_account" {
  source     = "../src/data/service_account"
  project_id = "cbh-sre"
  account_id = "sli-metrics"
}

module "ml_labeling_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cbh-ds"
  account_id = "ml-labeling"
}

module "cityblock_dev_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-development"
  account_id = "cityblock-development"
}

module "commons_prod_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "commons-production"
  account_id = "commons-production"
}

module "commons_staging_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "commons-183915"
  account_id = "commons-staging"
}

module "load_tufts_service_account" {
  source     = "../src/data/service_account"
  project_id =module.cityblock_orchestration_project_ref.project_id
  account_id = "load-daily-tufts"
}

module "load_emblem_service_account" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "tf-svc-load-monthly-emblem"
}

module "svc_acct_carefirst_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "carefirst-worker"
}

module "able_health_worker_svc_acct" {
  source = "../src/data/service_account"
  account_id = "tf-svc-able-health"
  project_id = module.cityblock_orchestration_project_ref.project_id
}
