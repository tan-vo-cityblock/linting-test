module "carefirst_project" {
  source     = "../src/resource/project"
  project_id = "cbh-carefirst-data"
  name       = "CareFirst Data"
  api_services = [
    // APIs for Endpoints: https://cloud.google.com/endpoints/docs/openapi/get-started-app-engine-standard#checking_required_services
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
  ]
}

module "cbh_dev_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-development"
}
