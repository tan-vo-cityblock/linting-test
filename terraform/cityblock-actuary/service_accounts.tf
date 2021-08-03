module "dbt_prod_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_analytics_prod_project_ref.project_id
  account_id = "dbt-prod"
}

module "dbt_staging_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_analytics_staging_project_ref.project_id
  account_id = "dbt-run-staging"
}
