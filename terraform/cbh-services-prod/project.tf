module "services_prod_project" {
  source              = "../src/resource/project"
  project_id          = "cbh-services-prod"
  name                = "Services Production"
  app_engine_location = "us-east4"
  api_services = [ // APIs for Endpoints: https://cloud.google.com/endpoints/docs/openapi/get-started-app-engine-standard#checking_required_services
    "appengine.googleapis.com",
    "appengineflex.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudtrace.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "containerregistry.googleapis.com",
    "deploymentmanager.googleapis.com",
    "endpoints.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "pubsub.googleapis.com",
    "replicapool.googleapis.com",
    "replicapoolupdater.googleapis.com",
    "resourceviews.googleapis.com",
    "servicecontrol.googleapis.com",
    "servicemanagement.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "sql-component.googleapis.com",
    "sqladmin.googleapis.com"
  ]
}
