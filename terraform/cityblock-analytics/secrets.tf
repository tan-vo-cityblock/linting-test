resource "google_service_account_key" "dbt_prod_key" {
  service_account_id = module.dbt_prod_svc_acct.name
}

module "dbt_k8s_secret" {
  source = "../src/resource/kubernetes"
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  secret_name = "tf-svc-${module.dbt_prod_svc_acct.account_id}"
  secret_data = {
    "key.json" = base64decode(google_service_account_key.dbt_prod_key.private_key)
  }
}
