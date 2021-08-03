module "able_health_svc_acct" {
  source = "../src/data/service_account"
  project_id    = module.cityblock_orchestration_project_ref.project_id
  account_id = "tf-svc-able-health"
}

module "looker_demo" {
  source = "../src/data/service_account"
  project_id    = module.cityblock_data_project_ref.project_id
  account_id = "looker-demo"
}

module "elation_worker_svc_acct" {
  source = "../src/data/service_account"
  account_id = "elation-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "dbt_staging_svc_acct" {
  source = "../src/data/service_account"
  project_id = module.cbh_analytics_staging_project_ref.project_id
  account_id = "dbt-run-staging"
}

module "dbt_prod_svc_acct" {
  source = "../src/data/service_account"
  project_id = module.cityblock_analytics_project_ref.project_id
  account_id = "dbt-prod"
}

module "ml_labeling_svc_acct" {
  source = "../src/data/service_account"
  project_id = module.cbh_ds_ref.project_id
  account_id = "ml-labeling"
}

module "development_dataflow_svc_acct" {
  source = "../src/data/service_account"
  account_id = "dataflow"
  project_id = module.cbh_dev_ref.project_id
}
