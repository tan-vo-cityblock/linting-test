resource "google_service_account_key" "sli_metrics_key" {
  service_account_id = module.sli_metrics_svc_acct.name
}

module "sli_metrics_k8s_secret" {
  source = "../src/resource/kubernetes"
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  secret_name = "tf-svc-${module.sli_metrics_svc_acct.account_id}"
  secret_data = {
    "key.json" = base64decode(google_service_account_key.sli_metrics_key.private_key)
  }
}
