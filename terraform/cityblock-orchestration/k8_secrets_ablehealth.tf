locals {
  kms_ablehealth_user_prod     = data.terraform_remote_state.cbh_kms_ref.outputs.ablehealth_user_prod     // map
  kms_ablehealth_pwd_prod      = data.terraform_remote_state.cbh_kms_ref.outputs.ablehealth_pwd_prod      // map
  kms_ablehealth_s3_creds_prod = data.terraform_remote_state.cbh_kms_ref.outputs.ablehealth_s3_creds_prod // map
  kms_qm_svc_able_api_key_prod = data.terraform_remote_state.cbh_kms_ref.outputs.qm_prod_api_key_able_key // map
}

// ablehealth user
module "ablehealth_user_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_ablehealth_user_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_ablehealth_user_prod.key_name}/${local.kms_ablehealth_user_prod.file_names.user_64_enc}"
}

data "google_kms_secret" "ablehealth_user_prod_secret" {
  crypto_key = local.kms_ablehealth_user_prod.self_link
  ciphertext = module.ablehealth_user_prod_base64_secret_object.object
}

// ablehealth password
module "ablehealth_pwd_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_ablehealth_pwd_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_ablehealth_pwd_prod.key_name}/${local.kms_ablehealth_pwd_prod.file_names.password_64_enc}"
}

data "google_kms_secret" "ablehealth_pwd_prod_secret" {
  crypto_key = local.kms_ablehealth_pwd_prod.self_link
  ciphertext = module.ablehealth_pwd_prod_base64_secret_object.object
}

// ablehealth S3 creds
module "ablehealth_s3_creds_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_ablehealth_s3_creds_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_ablehealth_s3_creds_prod.key_name}/${local.kms_ablehealth_s3_creds_prod.file_names.credentials_64_enc}"
}

data "google_kms_secret" "ablehealth_s3_creds_prod_secret" {
  crypto_key = local.kms_ablehealth_s3_creds_prod.self_link
  ciphertext = module.ablehealth_s3_creds_prod_base64_secret_object.object
}

// qm service able dag api key
module "able_to_cbh_dag_api_key_prod_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_qm_svc_able_api_key_prod.ring_name}-${local.secrets}"
  object_path   = "${local.kms_qm_svc_able_api_key_prod.key_name}/${local.kms_qm_svc_able_api_key_prod.file_names.api_key_able_64_enc}"
}

data "google_kms_secret" "able_to_cbh_dag_api_key_prod_base64_secret" {
  crypto_key = local.kms_qm_svc_able_api_key_prod.self_link
  ciphertext = module.able_to_cbh_dag_api_key_prod_base64_secret_object.object
}

module "ablehealth_prod_k8_user_pass" {
  source      = "../src/resource/kubernetes"
  secret_name = "ablehealth-prod-secrets"
  secret_data = {
    "${local.kms_ablehealth_user_prod.file_names.user}" : data.google_kms_secret.ablehealth_user_prod_secret.plaintext,
    "${local.kms_ablehealth_pwd_prod.file_names.password}" : data.google_kms_secret.ablehealth_pwd_prod_secret.plaintext,
    "${local.kms_ablehealth_s3_creds_prod.file_names.credentials}" : data.google_kms_secret.ablehealth_s3_creds_prod_secret.plaintext,
    "${local.kms_qm_svc_able_api_key_prod.file_names.api_key_able}" : data.google_kms_secret.able_to_cbh_dag_api_key_prod_base64_secret.plaintext
  }
}
