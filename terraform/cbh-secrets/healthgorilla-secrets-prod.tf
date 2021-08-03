module "prod_healthgorilla_api_creds_secret" {
  source = "../src/resource/secret_manager/secret"
  secret_id = "prod-healthgorilla-api-creds"
  secret_accessors = ["serviceAccount:${module.prod_healthgorilla_worker_svc_acct.email}"]
}
