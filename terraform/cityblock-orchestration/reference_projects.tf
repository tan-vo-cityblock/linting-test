module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}

module "cityblock_cold_storage_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-cold-storage"
}

module "reference_data_project_ref" {
  source     = "../src/data/project"
  project_id = "reference-data-199919"
}
