locals {
  kms_mem_svc_db_mirror_user_prod = data.terraform_remote_state.cbh_kms_ref.outputs.mem_svc_prod_db_mirror_user
  secrets                         = "secrets"
}


module "member_index_postgres_instance_prod" {
  source      = "../src/resource/cloudsql/instance"
  project_id  = var.partner_project_production
  name        = "member-index"
  enable_logs = false
  labels = {
    data = "phi"
  }
}


module "member_index_db_prod" {
  source        = "../src/resource/cloudsql/database"
  name          = "prod"
  project_id    = var.partner_project_production
  instance_name = module.member_index_postgres_instance_prod.name
}

//CREATE mem-svc-mirror-user for member-index instance
// pull username and password for qm-mirror-user, json decode, and create postgres user
module "mem_svc_db_mirror_user_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_mem_svc_db_mirror_user_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_mem_svc_db_mirror_user_prod.key_name}/${local.kms_mem_svc_db_mirror_user_prod.file_names.db_mirror_user_64_enc}"
}

data "google_kms_secret" "mem_svc_db_mirror_user_prod_secret" {
  crypto_key = local.kms_mem_svc_db_mirror_user_prod.self_link
  ciphertext = module.mem_svc_db_mirror_user_prod_base64_secret_object.object
}

// create sql qm-mirror-user
module "mem_svc_db_mirror_user_prod" {
  source        = "../src/resource/cloudsql/postgres_user"
  instance_name = module.member_index_postgres_instance_prod.name
  project_id    = var.partner_project_production
  name          = jsondecode(data.google_kms_secret.mem_svc_db_mirror_user_prod_secret.plaintext).username
  password      = jsondecode(data.google_kms_secret.mem_svc_db_mirror_user_prod_secret.plaintext).password
}
