// key ring
module "qm_service_ring_prod" {
  source = "../src/resource/kms/key_ring"
  name   = "cbh-quality-measure-prod" // Note quality-measure service lives in project id cbh-services-[env]
  crypto_key_decrypters = [
    "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}",
    "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}"
  ]
}

// app.yaml key
module "qm_service_prod_app_yaml_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.app_yaml_key_name
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

// app.yaml key output
output "qm_service_prod_app_yaml_key" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_service_prod_app_yaml_key.crypto_key_self_link
    key_name : module.qm_service_prod_app_yaml_key.name
    file_names : {
      app_quality_measure_yaml : "app.quality_measure.yaml"
      app_quality_measure_yaml_64_enc : "app.quality_measure.yaml.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

// api_key_commons key
module "qm_prod_api_key_commons_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.api-key-commons
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

// api_key_commons key output
output "qm_prod_api_key_commons_key" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_prod_api_key_commons_key.crypto_key_self_link
    key_name : module.qm_prod_api_key_commons_key.name
    file_names : {
      api_key_commons : "api_key.commons"
      api_key_commons_64_enc : "api_key.commons.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

// api_key_able key
module "qm_prod_api_key_able_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.api-key-able
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

// api_key_able key output
output "qm_prod_api_key_able_key" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_prod_api_key_able_key.crypto_key_self_link
    key_name : module.qm_prod_api_key_able_key.name
    file_names : {
      api_key_able : "api_key.able"
      api_key_able_64_enc : "api_key.able.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

// api_key_elation key
module "qm_prod_api_key_elation_key" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.api-key-elation
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

// api_key_elation key output
output "qm_prod_api_key_elation_key" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_prod_api_key_elation_key.crypto_key_self_link
    key_name : module.qm_prod_api_key_elation_key.name
    file_names : {
      api_key_elation : "api_key.elation"
      api_key_elation_64_enc : "api_key.elation.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

//db service user
module "qm_prod_db_service_user" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db-service-user
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

output "qm_prod_db_service_user" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_prod_db_service_user.crypto_key_self_link
    key_name : module.qm_prod_db_service_user.name
    file_names : {
      db_service_user : local.db-service-user
      db_service_user_64_enc : "${local.db-service-user}.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

//db mirror user
module "qm_prod_db_mirror_user" {
  source   = "../src/resource/kms/crypto_key"
  name     = local.db-mirror-user
  key_ring = module.qm_service_ring_prod.key_ring_self_link
}

output "qm_prod_db_mirror_user" {
  value = {
    ring_name : module.qm_service_ring_prod.name
    self_link : module.qm_prod_db_mirror_user.crypto_key_self_link
    key_name : module.qm_prod_db_mirror_user.name
    file_names : {
      db_mirror_user : local.db-mirror-user
      db_mirror_user_64_enc : "${local.db-mirror-user}.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
