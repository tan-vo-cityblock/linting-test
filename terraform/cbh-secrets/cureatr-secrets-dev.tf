module "dev_cureatr_api_creds_secret_mgr_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "dev-cureatr-api-creds"
  secret_accessors = [
    "serviceAccount:${module.dev_cureatr_worker_svc_acct.email}",
  ]
}