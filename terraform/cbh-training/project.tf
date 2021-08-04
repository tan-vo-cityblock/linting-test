module "training_project" {
  source     = "../src/resource/project"
  name       = "Cityblock Training"
  project_id = "cbh-training"
  api_services = [
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com"
  ]
}
