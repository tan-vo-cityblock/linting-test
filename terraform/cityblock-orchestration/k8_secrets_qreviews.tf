locals {
  kms_qreviews_prod_creds     = data.terraform_remote_state.cbh_kms_ref.outputs.qreviews_creds
  qreviews_creds_secrets = jsondecode(data.google_kms_secret.qreviews_prod_creds_secret.plaintext)
}

module "qreviews_prod_creds_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_qreviews_prod_creds.ring_name}-secrets"
  object_path   = "${local.kms_qreviews_prod_creds.key_name}/${local.kms_qreviews_prod_creds.file_names.api_auth_64_enc}"
}

data "google_kms_secret" "qreviews_prod_creds_secret" {
  crypto_key = local.kms_qreviews_prod_creds.self_link
  ciphertext = module.qreviews_prod_creds_base64_secret_object.object
}

module "qreviews_prod_k8s_secrets" {
  source      = "../src/resource/kubernetes"
  secret_name = "qreviews-prod-secrets"
  secret_data = {
    "api_account_sid" : local.qreviews_creds_secrets.api_account_sid,
    "api_key" : local.qreviews_creds_secrets.api_key
  }
}
