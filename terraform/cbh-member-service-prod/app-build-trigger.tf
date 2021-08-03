data "terraform_remote_state" "cbh_kms_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/kms"
  }
}

module "member_service_app_prod" {
  source                   = "../src/custom/member-service"
  project_id               = module.member_service_prod_project.project_id
  database_project         = "cityblock-data"
  cloud_sql_instance       = "cityblock-data:us-east1:member-index"
  deployment_tag_name      = "^member-service-(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)$"
  project_key_ring_name    = data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_prod_name
  app_yaml_key_name        = data.terraform_remote_state.cbh_kms_ref.outputs.app_yaml_key_name
  db_password_key_name     = data.terraform_remote_state.cbh_kms_ref.outputs.db_password_key_name
  app_engine_svc_acct_name = data.google_app_engine_default_service_account.app_engine_svc_acct.name
}
