module "load_monthly_emblem_svc_acct" {
  source     = "../src/data/service_account"
  project_id = "cityblock-orchestration"
  account_id = "tf-svc-load-monthly-emblem"
}

module "dbt_staging_svc_acct" {
  source = "../src/data/service_account"
  project_id = "cbh-analytics-staging"
  account_id = "dbt-run-staging"
}

module "dbt_prod_svc_acct" {
  source = "../src/data/service_account"
  project_id = "cityblock-analytics"
  account_id = "dbt-prod"
}

module "development_dataflow_svc_acct" {
  source = "../src/data/service_account"
  account_id = "dataflow"
  project_id = module.cbh_dev_ref.project_id
}
