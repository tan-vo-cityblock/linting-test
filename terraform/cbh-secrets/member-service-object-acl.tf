//TODO: Migrate to bucket and delete

module "member_service_staging_app_yaml_acl" {
  source        = "../src/resource/storage/object_acl"
  object_bucket = module.cbh_secrets_bucket.name
  object_path   = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_staging_name}/${data.terraform_remote_state.cbh_kms_ref.outputs.app_yaml_key_name}/app.yaml.enc"
  role_entities = [
    "READER:user-${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "READER:user-${module.staging_cityblock_data_project_ref.default_compute_engine_service_account_email}"
  ]
}

module "member_service_staging_db_password_acl" {
  source        = "../src/resource/storage/object_acl"
  object_bucket = module.cbh_secrets_bucket.name
  object_path   = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_staging_name}/${data.terraform_remote_state.cbh_kms_ref.outputs.db_password_key_name}/db_password.txt.enc"
  role_entities = [
    "READER:user-${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}

module "member_service_prod_app_yaml_acl" {
  source        = "../src/resource/storage/object_acl"
  object_bucket = module.cbh_secrets_bucket.name
  object_path   = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_prod_name}/${data.terraform_remote_state.cbh_kms_ref.outputs.app_yaml_key_name}/app.yaml.enc"
  role_entities = [
    "READER:user-${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "READER:user-${module.cityblock_data_project_ref.default_compute_engine_service_account_email}"
  ]
}

module "member_service_prod_db_password_acl" {
  source        = "../src/resource/storage/object_acl"
  object_bucket = module.cbh_secrets_bucket.name
  object_path   = "${data.terraform_remote_state.cbh_kms_ref.outputs.member_service_ring_prod_name}/${data.terraform_remote_state.cbh_kms_ref.outputs.db_password_key_name}/db_password.txt.enc"
  role_entities = [
    "READER:user-${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
  ]
}
