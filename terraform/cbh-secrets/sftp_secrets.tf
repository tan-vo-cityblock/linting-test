module "sftp_prod_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "sftp_key_prod"
  secret_accessors = [
    "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}"
  ]
}

module "sftp_staging_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "sftp_key_staging"
}
