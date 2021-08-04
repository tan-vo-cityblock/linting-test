// Elation
module "elation_ring_prod" {
  source                = "../src/resource/kms/key_ring"
  name                  = "cbh-elation-prod"
  crypto_key_decrypters = []
}

module "elation_ssh_private_key_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.ssh-private-key
  key_ring = module.elation_ring_prod.key_ring_self_link
}

module "elation_ssh_public_key_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.ssh-public-key
  key_ring = module.elation_ring_prod.key_ring_self_link
}

module "elation_ssh_config_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.ssh-config
  key_ring = module.elation_ring_prod.key_ring_self_link
}

module "elation_known_hosts_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.known-hosts
  key_ring = module.elation_ring_prod.key_ring_self_link
}

module "elation_mysql_config_prod" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.mysql-config
  key_ring = module.elation_ring_prod.key_ring_self_link
}

