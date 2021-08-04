module "scio_jobs_app_conf_secret_manager_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "scio-jobs-application-conf"
  secret_accessors = [
    "serviceAccount:${module.redox_worker_svc_acct.email}",
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}
