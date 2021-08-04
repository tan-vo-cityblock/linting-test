// quality measure db module
module "quality_measure_db" {
  source        = "../src/resource/cloudsql/database"
  name          = "quality_measure"
  project_id    = module.services_staging_project.project_id
  instance_name = module.services_postgres_instance.name
}

// use remote state cbh_secrets_ref for pulling db user passwords

locals {
  staging_qm_service_secret_ids = data.terraform_remote_state.cbh_secrets_ref.outputs.staging_qm_service_secret_ids
  latest                        = "latest"
}

// pull username and password for qm service user and create postgres user

data "google_secret_manager_secret_version" "staging_qm_service_db_service_user_name" {
  project = module.cbh_secrets_ref.project_number
  secret  = local.staging_qm_service_secret_ids.db_service_user_name
  version = local.latest
}

data "google_secret_manager_secret_version" "staging_qm_service_db_service_user_password" {
  project = module.cbh_secrets_ref.project_number
  secret  = local.staging_qm_service_secret_ids.db_service_user_password
  version = local.latest
}

// create sql qm service user
module "qm_svc_db_service_user" {
  source        = "../src/resource/cloudsql/postgres_user"
  instance_name = module.services_postgres_instance.name
  project_id    = module.services_staging_project.project_id
  name          = data.google_secret_manager_secret_version.staging_qm_service_db_service_user_name.secret_data
  password      = data.google_secret_manager_secret_version.staging_qm_service_db_service_user_password.secret_data
}

// pull username and password for qm mirror user and create postgres user

data "google_secret_manager_secret_version" "staging_qm_service_db_mirror_user_name" {
  project = module.cbh_secrets_ref.project_number
  secret  = local.staging_qm_service_secret_ids.db_mirror_user_name
  version = local.latest
}

data "google_secret_manager_secret_version" "staging_qm_service_db_mirror_user_password" {
  project = module.cbh_secrets_ref.project_number
  secret  = local.staging_qm_service_secret_ids.db_mirror_user_password
  version = local.latest
}

// create sql qm mirror user
module "qm_svc_db_mirror_user" {
  source        = "../src/resource/cloudsql/postgres_user"
  instance_name = module.services_postgres_instance.name
  project_id    = module.services_staging_project.project_id
  name          = data.google_secret_manager_secret_version.staging_qm_service_db_mirror_user_name.secret_data
  password      = data.google_secret_manager_secret_version.staging_qm_service_db_mirror_user_password.secret_data
}
