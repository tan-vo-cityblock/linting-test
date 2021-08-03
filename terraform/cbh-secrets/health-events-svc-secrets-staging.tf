// db - service user creds
module "staging_health_events_service_db_user_name" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-health-events-service-db-user-name"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
  ]
}

module "staging_health_events_service_db_user_password" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-health-events-service-db-user-password"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

module "staging_health_events_service_api_key" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "staging-health-events-service-api-key"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

output "staging_health_events_service_secret_ids" {
  value = {
    db_service_user_name : module.staging_health_events_service_db_user_name.secret_id
    db_service_user_password : module.staging_health_events_service_db_user_password.secret_id
    api_key : module.staging_health_events_service_api_key.secret_id
  }
  description = "Map containing secret_id values for each secret module"
  sensitive   = true
}