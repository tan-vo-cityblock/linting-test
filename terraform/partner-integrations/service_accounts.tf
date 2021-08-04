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
