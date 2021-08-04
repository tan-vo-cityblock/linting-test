module "commons_github_ring" {
  source                = "../src/resource/kms/key_ring"
  name                  = "cbh-commons-github"
  crypto_key_decrypters = []
}

module "commons_github_creds" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.commons-github-token
  key_ring = module.commons_github_ring.key_ring_self_link
}
