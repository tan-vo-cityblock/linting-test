locals {
  api_key_mem_svc       = "api-key-mem-svc"
  elation_client_id     = "elation-client-id"
  elation_client_secret = "elation-client-secret"
  elation_pass          = "elation-pass"
}

module "member_service_staging_project_ref" {
  source     = "../src/data/project"
  project_id = "cbh-member-service-staging"
}

module "member_service_ring_staging" {
  source = "../src/resource/kms/key_ring"
  name   = module.member_service_staging_project_ref.project_id
  crypto_key_decrypters = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "serviceAccount:${module.staging_cityblock_data_project_ref.default_compute_engine_service_account_email}"
  ]
}

module "member_service_staging_app_yaml_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.app_yaml_key_name
  key_ring = module.member_service_ring_staging.key_ring_self_link
}

module "member_service_staging_db_password" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db_password
  key_ring = module.member_service_ring_staging.key_ring_self_link
}

//db mirror user
module "mem_svc_staging_db_mirror_user_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db-mirror-user
  key_ring = module.member_service_ring_staging.key_ring_self_link
}

output "mem_svc_staging_db_mirror_user" {
  value = {
    ring_name : module.member_service_ring_staging.name
    self_link : module.mem_svc_staging_db_mirror_user_key.crypto_key_self_link
    key_name : module.mem_svc_staging_db_mirror_user_key.name
    file_names : {
      db_mirror_user : local.db-mirror-user
      db_mirror_user_64_enc : "${local.db-mirror-user}.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
