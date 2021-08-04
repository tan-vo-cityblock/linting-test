module "cityblock_analytics_prod_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-analytics"
}

module "cityblock_analytics_staging_project_ref" {
  source     = "../src/data/project"
  project_id = "cbh-analytics-staging"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}
