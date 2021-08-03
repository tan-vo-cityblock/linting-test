locals {
  kms_commons_github_creds     = data.terraform_remote_state.cbh_kms_ref.outputs.commons_github_creds
  commons_github_creds_secrets = jsondecode(data.google_kms_secret.commons_github_creds_secret.plaintext)
}

module "commons_github_creds_base64_secret_object" {
  source        = "../src/custom/storage_object"
  object_bucket = "${local.kms_commons_github_creds.ring_name}-secrets"
  object_path   = "${local.kms_commons_github_creds.key_name}/${local.kms_commons_github_creds.file_names.commons_github_token_64_enc}"
}

data "google_kms_secret" "commons_github_creds_secret" {
  crypto_key = local.kms_commons_github_creds.self_link
  ciphertext = module.commons_github_creds_base64_secret_object.object
}

module "commons_github_k8s_secrets" {
  source      = "../src/resource/kubernetes"
  secret_name = "commons-github-secrets"
  secret_data = {
    "github_token" : local.commons_github_creds_secrets.token,
  }
}
