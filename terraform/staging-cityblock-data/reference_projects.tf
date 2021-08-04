module "cbh_member_service_staging_ref" {
  source     = "../src/data/project"
  project_id = "cbh-member-service-staging"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}
