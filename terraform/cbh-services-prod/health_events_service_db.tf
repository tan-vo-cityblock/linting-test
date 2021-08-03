module "health_events_db" {
  source        = "../src/resource/cloudsql/database"
  name          = "health_events"
  project_id    = module.services_prod_project.project_id
  instance_name = module.services_postgres_instance.name
}

# // create sql health events service user
module "health_events_svc_db_service_user" {
  source        = "../src/resource/cloudsql/postgres_user"
  instance_name = module.services_postgres_instance.name
  project_id    = module.services_prod_project.project_id
  name          = data.google_secret_manager_secret_version.prod_health_events_service_db_user_name.secret_data
  password      = data.google_secret_manager_secret_version.prod_health_events_service_db_user_password.secret_data
}

locals {
  prod_health_events_service_secret_ids = data.terraform_remote_state.cbh_secrets_ref.outputs.prod_health_events_service_secret_ids
  hes_latest                        = "latest"
}

data "google_secret_manager_secret_version" "prod_health_events_service_db_user_name" {
  project = module.cbh_secrets_ref.project_number 
  secret  = local.prod_health_events_service_secret_ids.db_service_user_name
  version = local.hes_latest
}

data "google_secret_manager_secret_version" "prod_health_events_service_db_user_password" {
  project = module.cbh_secrets_ref.project_number
  secret  = local.prod_health_events_service_secret_ids.db_service_user_password
  version = local.hes_latest
}
