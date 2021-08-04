module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}

module "cbh_analytics_staging_project_ref" {
  source     = "../src/data/project"
  project_id = "cbh-analytics-staging"
}

module "cityblock_analytics_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-analytics"
}

module "cbh_dev_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-development"
}
