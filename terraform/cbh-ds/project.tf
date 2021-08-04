module "data_science_project" {
  source     = "../src/resource/project"
  project_id = "cbh-ds"
  name       = "Data Science"
  api_services = [
    // APIs for Endpoints: https://cloud.google.com/endpoints/docs/openapi/get-started-app-engine-standard#checking_required_services
    "bigquery.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com"
  ]
}
