module "db_mirror_prod" {
  source     = "../src/resource/project"
  project_id = "cbh-db-mirror-prod"
  name       = "Database Mirrors Prod"
  api_services = [
    // APIs for Endpoints: https://cloud.google.com/endpoints/docs/openapi/get-started-app-engine-standard#checking_required_services
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "pubsub.googleapis.com",
    "resourceviews.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "containerregistry.googleapis.com",
    "container.googleapis.com",
    "deploymentmanager.googleapis.com",
    "sql-component.googleapis.com",    // necessary for service accounts to connect to Cloud SQL API
    "sqladmin.googleapis.com"
  ]
}
