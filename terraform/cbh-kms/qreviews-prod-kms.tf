// key ring
module "qreviews_prod_ring" {
  source                = "../src/resource/kms/key_ring"
  name                  = "qreviews-prod"
  crypto_key_decrypters = []
}

module "qreviews_creds" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.login-creds
  key_ring = module.qreviews_prod_ring.key_ring_self_link
}

output "qreviews_creds" {
  value = {
    ring_name : module.qreviews_prod_ring.name
    self_link : module.qreviews_creds.crypto_key_self_link
    key_name : module.qreviews_creds.name
    file_names : {
      api_auth : "api_auth",
      api_auth_64_enc : "api_auth.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
