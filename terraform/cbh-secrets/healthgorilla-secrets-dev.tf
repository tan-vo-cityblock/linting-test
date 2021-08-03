module "dev_healthgorilla_api_creds_secret" {
  source = "../src/resource/secret_manager/secret"
  secret_id = "dev-healthgorilla-api-creds"
  secret_accessors = ["serviceAccount:${module.dev_healthgorilla_worker_svc_acct.email}"]
}
