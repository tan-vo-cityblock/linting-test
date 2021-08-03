// app.yaml
module "staging_member_service_app_yaml" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-app-yaml"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "serviceAccount:${module.staging_cityblock_data_project_ref.default_compute_engine_service_account_email}",
    "serviceAccount:${module.prod_commons_mirror_svc_acct.email}",
    "serviceAccount:${module.staging_commons_mirror_svc_acct.email}",
    "serviceAccount:${module.prod_quality_measure_mirror_svc_acct.email}",
    "serviceAccount:${module.prod_member_index_mirror_svc_acct.email}",
    "serviceAccount:${module.staging_member_index_mirror_svc_acct.email}",
    "serviceAccount:${module.redox_worker_svc_acct.email}",
  ]
}

// api_key - commons
module "staging_member_service_api_key_commons" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-api-key-commons"
}

// db - service user creds
module "staging_member_service_db_service_user_name" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-db-service-user-name"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
  ]
}

module "staging_member_service_db_service_user_password" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-db-service-user-password"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

// db - mirror user creds
module "staging_member_service_db_mirror_user_name" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-db-mirror-user-name"
}

module "staging_member_service_db_mirror_user_name_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source    = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id = module.staging_member_service_db_mirror_user_name.secret_id
}

module "staging_member_service_db_mirror_user_password" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-member-service-db-mirror-user-password"
}

module "staging_member_service_db_mirror_user_password_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source    = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id = module.staging_member_service_db_mirror_user_password.secret_id
}

output "staging_member_service_secret_ids" {
  value = {
    app_yaml : module.staging_member_service_app_yaml.secret_id
    db_service_user_name : module.staging_member_service_db_service_user_name.secret_id
    db_service_user_password : module.staging_member_service_db_service_user_password.secret_id
    db_mirror_user_name : module.staging_member_service_db_mirror_user_name.secret_id
    db_mirror_user_password : module.staging_member_service_db_mirror_user_password.secret_id
  }
  description = "Map containing secret_id values for each secret module"
  sensitive   = true
}
