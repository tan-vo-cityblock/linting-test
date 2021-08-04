locals {
  kms_mem_svc_db_mirror_user_staging = data.terraform_remote_state.cbh_kms_ref.outputs.mem_svc_staging_db_mirror_user
  secrets = "secrets"
}

module "member_index_postgres_instance_staging" {
  source = "../src/resource/cloudsql/instance"
  project_id = var.project_staging
  name = "member-index"
  region = "us-central1"
  enable_logs = false
}

module "member_index_db_staging" {
  source = "../src/resource/cloudsql/database"
  name = "staging"
  project_id = var.project_staging
  instance_name = module.member_index_postgres_instance_staging.name
}

//CREATE mem-svc-mirror-user for member-index instance
// pull username and password for qm-mirror-user, json decode, and create postgres user
module "mem_svc_db_mirror_user_staging_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_mem_svc_db_mirror_user_staging.ring_name}-${local.secrets}"
  object_path   = "${local.kms_mem_svc_db_mirror_user_staging.key_name}/${local.kms_mem_svc_db_mirror_user_staging.file_names.db_mirror_user_64_enc}"
}

data "google_kms_secret" "mem_svc_db_mirror_user_staging_secret" {
  crypto_key = local.kms_mem_svc_db_mirror_user_staging.self_link
  ciphertext = module.mem_svc_db_mirror_user_staging_base64_secret_object.object
}

// create sql qm-mirror-user
module "mem_svc_db_mirror_user_staging" {
  source = "../src/resource/cloudsql/postgres_user"
  instance_name = module.member_index_postgres_instance_staging.name
  project_id =  var.project_staging
  name = jsondecode(data.google_kms_secret.mem_svc_db_mirror_user_staging_secret.plaintext).username
  password = jsondecode(data.google_kms_secret.mem_svc_db_mirror_user_staging_secret.plaintext).password
}
