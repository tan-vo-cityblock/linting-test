module "staging_datadog_secret_mgr_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-datadog-key"
}

module "staging_datadog_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source         = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id      = module.staging_datadog_secret_mgr_secret.secret_id
}
