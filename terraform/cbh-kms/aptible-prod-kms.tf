module "aptible_prod_ring" {
  source                = "../src/resource/kms/key_ring"
  name                  = "cbh-aptible-prod"
  crypto_key_decrypters = []
}

module "aptible_prod_creds" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.login-creds
  key_ring = module.aptible_prod_ring.key_ring_self_link
}

output "aptible_prod_creds" {
  value = {
    ring_name : module.aptible_prod_ring.name
    self_link : module.aptible_prod_creds.crypto_key_self_link
    key_name : module.aptible_prod_creds.name
    file_names : {
      login_creds : "login_creds",
      login_creds_64_enc : "login_creds.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
