locals {
  kms_aptible_prod_creds     = data.terraform_remote_state.cbh_kms_ref.outputs.aptible_prod_creds
  aptible_prod_creds_secrets = jsondecode(data.google_kms_secret.aptible_prod_creds_secret.plaintext)
}

module "aptible_prod_creds_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_aptible_prod_creds.ring_name}-secrets"
  object_path   = "${local.kms_aptible_prod_creds.key_name}/${local.kms_aptible_prod_creds.file_names.login_creds_64_enc}"
}

data "google_kms_secret" "aptible_prod_creds_secret" {
  crypto_key = local.kms_aptible_prod_creds.self_link
  ciphertext = module.aptible_prod_creds_base64_secret_object.object
}

module "aptible_prod_k8s_secrets" {
  source      = "../src/resource/kubernetes"
  secret_name = "aptible-prod-secrets"
  secret_data = {
    "username" : local.aptible_prod_creds_secrets.username,
    "password" : local.aptible_prod_creds_secrets.password
  }
}
