module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}
