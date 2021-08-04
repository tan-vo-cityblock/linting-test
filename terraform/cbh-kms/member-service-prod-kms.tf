module "member_service_prod_project_ref" {
  source     = "../src/data/project"
  project_id = "cbh-member-service-prod"
}

module "member_service_ring_prod" {
  source = "../src/resource/kms/key_ring"
  name   = module.member_service_prod_project_ref.project_id
  crypto_key_decrypters = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}",
    "serviceAccount:${module.load_cci_service_account.email}",
    "serviceAccount:${module.load_emblem_service_account.email}",
    "serviceAccount:${module.load_emblem_pbm_service_account.email}",
    "serviceAccount:${module.load_tufts_daily_service_account.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}",
    "serviceAccount:${module.elation_worker_svc_acct.email}",
    "serviceAccount:${module.svc_acct_cardinal_worker.email}",
    "serviceAccount:${module.svc_acct_healthyblue_worker.email}",
    "user:caitlin.dugan@cityblock.com",
    "user:katie.claiborne@cityblock.com",
    "serviceAccount:${module.prod_commons_mirror_svc_acct.email}",
    "serviceAccount:${module.staging_commons_mirror_svc_acct.email}",
    "serviceAccount:${module.prod_quality_measure_mirror_svc_acct.email}",
    "serviceAccount:${module.prod_member_index_mirror_svc_acct.email}",
    "serviceAccount:${module.staging_member_index_mirror_svc_acct.email}",
    "serviceAccount:${module.redox_worker_svc_acct.email}",
    "serviceAccount:${module.payer_suspect_svc_acct.email}"

  ]
}

module "member_service_prod_app_yaml_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.app_yaml_key_name
  key_ring = module.member_service_ring_prod.key_ring_self_link
}

module "member_service_prod_db_password" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db_password
  key_ring = module.member_service_ring_prod.key_ring_self_link
}

//db mirror user
module "mem_svc_prod_db_mirror_user_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db-mirror-user
  key_ring = module.member_service_ring_prod.key_ring_self_link
}

output "mem_svc_prod_db_mirror_user" {
  value = {
    ring_name : module.member_service_ring_prod.name
    self_link : module.mem_svc_prod_db_mirror_user_key.crypto_key_self_link
    key_name : module.mem_svc_prod_db_mirror_user_key.name
    file_names : {
      db_mirror_user : local.db-mirror-user
      db_mirror_user_64_enc : "${local.db-mirror-user}.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
