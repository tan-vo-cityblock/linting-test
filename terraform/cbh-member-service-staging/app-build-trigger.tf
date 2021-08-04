// TODO rename to member_service_app_staging and apply terraform mv
module "member_service_staging_app" {
  source                   = "../src/custom/member-service"
  project_id               = module.member_service_staging_project.project_id
  database_project         = "staging-cityblock-data"
  cloud_sql_instance       = "cbh-services-staging:us-east1:services"
  deployment_branch_name   = "^master$"
  project_key_ring_name    = data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_staging_name
  app_yaml_key_name        = data.terraform_remote_state.cbh_kms_ref.outputs.app_yaml_key_name
  db_password_key_name     = data.terraform_remote_state.cbh_kms_ref.outputs.db_password_key_name
  app_engine_svc_acct_name = data.google_app_engine_default_service_account.app_engine_svc_acct.name
}
