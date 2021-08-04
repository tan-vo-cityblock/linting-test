module "development_project" {
  source     = "../src/resource/project"
  name       = "Cityblock Development"
  project_id = "cityblock-development"
  api_services = [
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "bigquery.googleapis.com",
    "dataflow.googleapis.com"
  ]
}
