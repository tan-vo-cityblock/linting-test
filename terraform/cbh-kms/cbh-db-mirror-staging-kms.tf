module "cbh_db_mirror_staging_ring" {
  source                = "../src/resource/kms/key_ring"
  name                  = "cbh-db-mirror-staging"
  crypto_key_decrypters = []
}

// Update below output to have more data when a key gets made on the above ring.
output "cbh_db_mirror_staging" {
  value = {
    ring_name : module.cbh_db_mirror_staging_ring.name
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
