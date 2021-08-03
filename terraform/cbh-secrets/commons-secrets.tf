module "commons_build_staging_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "commons_build_staging"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

module "staging_load_test_patient_secret" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-load-test-patient-id"
}

module "staging_load_test_patient_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source         = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id      = module.staging_load_test_patient_secret.secret_id
}
