output "dbt_ring_prod_name" {
  value       = module.dbt_ring_prod.name
  description = "ring name provided as remote output for use in cbh-dbt-prod-secrets bucket creation"
  sensitive   = true
}

module "dbt_ring_prod" {
  source = "../src/resource/kms/key_ring"
  name   = "cbh-dbt-prod"
  crypto_key_decrypters = [
    "serviceAccount:${module.cityblock_orchestration_project_ref.default_cloudbuild_service_account_email}"
  ]
}

module "dbt_prod_key_json" {
  source   = "../src/resource/kms/crypto_key"
  name     = "service-account-json"
  key_ring = module.dbt_ring_prod.key_ring_self_link
}
