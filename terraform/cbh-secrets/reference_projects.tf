module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}

module "staging_cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "staging-cityblock-data"
}

module "cityblock_analytics_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-analytics"
}

module "cbh_db_mirror_prod_ref" {
  source = "../src/data/project"
  project_id = "cbh-db-mirror-prod"
}

module "cbh_db_mirror_staging_ref" {
  source = "../src/data/project"
  project_id = "cbh-db-mirror-staging"
}
