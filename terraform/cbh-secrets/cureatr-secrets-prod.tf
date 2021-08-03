module "prod_cureatr_api_creds_secret_mgr_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-cureatr-api-creds"
  secret_accessors = [
    "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}",
  ]
}
