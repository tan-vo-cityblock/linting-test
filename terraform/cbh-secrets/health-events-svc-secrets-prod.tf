// db - service user creds
module "prod_health_events_service_db_user_name" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-health-events-service-db-user-name"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
  ]
}

module "prod_health_events_service_db_user_password" {
  source    = "../src/resource/secret_manager/secret"
  secret_id = "prod-health-events-service-db-user-password"
  secret_accessors = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

output "prod_health_events_service_secret_ids" {
  value = {
    db_service_user_name : module.prod_health_events_service_db_user_name.secret_id
    db_service_user_password : module.prod_health_events_service_db_user_password.secret_id
  }
  description = "Map containing secret_id values for each secret module"
  sensitive   = true
}