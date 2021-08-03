module "prod_zendesk_api_creds_secret_mgr_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-zendesk-api-creds"
  secret_accessors = [
    "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}",
  ]
}

module "prod_zendesk_api_creds_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source         = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id      = module.prod_zendesk_api_creds_secret_mgr_secret.secret_id
}


module "prod_zendesk_zendesk_worker_key_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-zendesk-worker-key"
  secret_accessors = [
    "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}",
  ]
}

