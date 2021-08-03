module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}

module "cityblock_cold_storage_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-cold-storage"
}

module "cbh_services_staging_ref" {
  source     = "../src/data/project"
  project_id = "cbh-services-staging"
}

module "cbh_services_prod_ref" {
  source     = "../src/data/project"
  project_id = "cbh-services-prod"
}

module "cbh_member_service_staging_ref" {
  source     = "../src/data/project"
  project_id = "cbh-member-service-staging"
}

module "cbh_member_service_prod_ref" {
  source     = "../src/data/project"
  project_id = "cbh-member-service-prod"
}

module "cbh_ds_ref" {
  source     = "../src/data/project"
  project_id = "cbh-ds"
}

module "cbh_dev_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-development"
}
