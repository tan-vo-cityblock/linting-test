module "prod_ge_slack_webhook_secret_mgr_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-ge-slack-webhook"
  secret_accessors = [
    "serviceAccount:${module.great_expectations_svc_acct.email}",
  ]
}

module "prod_ge_slack_webhook_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source         = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id      = module.prod_ge_slack_webhook_secret_mgr_secret.secret_id
}
