module "ablehealth_ring_prod" {
  source                = "../src/resource/kms/key_ring"
  name                  = "cbh-ablehealth-prod"
  crypto_key_decrypters = []
}

module "ablehealth_user_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.user
  key_ring = module.ablehealth_ring_prod.key_ring_self_link
}

module "ablehealth_pwd_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.password
  key_ring = module.ablehealth_ring_prod.key_ring_self_link
}

module "ablehealth_s3_creds" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.s3-creds
  key_ring = module.ablehealth_ring_prod.key_ring_self_link
}
