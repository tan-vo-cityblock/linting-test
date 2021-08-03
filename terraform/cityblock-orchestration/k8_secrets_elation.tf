// Modules here are for adding secrets to k8s clusters.
// NOTE: `data "google_kms_secret" "" {}` are NOT modularized so as not to expose exported attribute "plaintext" as an output.

locals {
  kms_elation_private_key_prod    = data.terraform_remote_state.cbh_kms_ref.outputs.elation_ssh_private_key_prod // map
  kms_elation_ssh_config_prod     = data.terraform_remote_state.cbh_kms_ref.outputs.elation_ssh_config_prod      // map
  kms_elation_known_hosts         = data.terraform_remote_state.cbh_kms_ref.outputs.elation_known_hosts_prod     // map
  kms_elation_mysql_config_prod   = data.terraform_remote_state.cbh_kms_ref.outputs.elation_mysql_config_prod    // map
  kms_qm_svc_elation_api_key_prod = data.terraform_remote_state.cbh_kms_ref.outputs.qm_prod_api_key_elation_key  // map
  secrets                         = "secrets"
}

// elation private key prod
module "elation_private_key_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_elation_private_key_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_elation_private_key_prod.key_name}/${local.kms_elation_private_key_prod.file_names.id_ed25519_cbh_elation_64_enc}"
}

data "google_kms_secret" "elation_private_key_prod_secret" {
  crypto_key = local.kms_elation_private_key_prod.self_link
  ciphertext = module.elation_private_key_prod_base64_secret_object.object
}

// elation ssh config prod
module "elation_ssh_config_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_elation_ssh_config_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_elation_ssh_config_prod.key_name}/${local.kms_elation_ssh_config_prod.file_names.ssh_config_64_enc}"
}

data "google_kms_secret" "elation_ssh_config_prod_secret" {
  crypto_key = local.kms_elation_ssh_config_prod.self_link
  ciphertext = module.elation_ssh_config_prod_base64_secret_object.object
}

// elation known hosts prod
module "elation_known_hosts_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_elation_known_hosts.ring_name}-${local.secrets}"
  object_path   = "${local.kms_elation_known_hosts.key_name}/${local.kms_elation_known_hosts.file_names.known_hosts_64_enc}"
}

data "google_kms_secret" "elation_known_hosts_prod_secret" {
  crypto_key = local.kms_elation_known_hosts.self_link
  ciphertext = module.elation_known_hosts_prod_base64_secret_object.object
}

//elation mysql config
module "elation_mysql_config_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_elation_mysql_config_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_elation_mysql_config_prod.key_name}/${local.kms_elation_mysql_config_prod.file_names.my_cnf_64_enc}"
}

data "google_kms_secret" "elation_mysql_config_prod_secret" {
  crypto_key = local.kms_elation_mysql_config_prod.self_link
  ciphertext = module.elation_mysql_config_prod_base64_secret_object.object
}

// qm service elation api key for DAG elation_mirror_prod_v1
module "elation_to_qm_svc_task_api_key_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_qm_svc_elation_api_key_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_qm_svc_elation_api_key_prod.key_name}/${local.kms_qm_svc_elation_api_key_prod.file_names.api_key_elation_64_enc}"
}

data "google_kms_secret" "elation_to_qm_svc_task_api_key_prod_base64_secret" {
  crypto_key = local.kms_qm_svc_elation_api_key_prod.self_link
  ciphertext = module.elation_to_qm_svc_task_api_key_prod_base64_secret_object.object
}

// k8 secret loading
// Note: keys in maps need to be wrapped if pulled from object property :/
module "elation_prod_k8_secrets" {
  source      = "../src/resource/kubernetes"
  secret_name = "elation-prod-secrets"
  secret_data = {
    "${local.kms_elation_private_key_prod.file_names.id_ed25519_cbh_elation}" : data.google_kms_secret.elation_private_key_prod_secret.plaintext,
    "${local.kms_elation_ssh_config_prod.file_names.ssh_config}" : data.google_kms_secret.elation_ssh_config_prod_secret.plaintext,
    "${local.kms_elation_known_hosts.file_names.known_hosts}" : data.google_kms_secret.elation_known_hosts_prod_secret.plaintext,
    "${local.kms_elation_mysql_config_prod.file_names.my_cnf}" : data.google_kms_secret.elation_mysql_config_prod_secret.plaintext
    "${local.kms_qm_svc_elation_api_key_prod.file_names.api_key_elation}" : data.google_kms_secret.elation_to_qm_svc_task_api_key_prod_base64_secret.plaintext
  }
}
