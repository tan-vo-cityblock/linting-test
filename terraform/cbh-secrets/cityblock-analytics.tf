module "dbt_prod_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_analytics_project_ref.project_id
  account_id = "dbt-prod"
}

resource "google_service_account_key" "dbt_prod_key" {
  service_account_id = module.dbt_prod_svc_acct.name
}

module "dbt_secret_manager_secret" {
  source     = "../src/resource/secret_manager/secret"
  secret_id  = "dbt_prod"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

module "dbt_secret_manager_version" {
  source      = "../src/resource/secret_manager/version"
  secret_id   = module.dbt_secret_manager_secret.id
  secret_data = base64decode(google_service_account_key.dbt_prod_key.private_key)
}
