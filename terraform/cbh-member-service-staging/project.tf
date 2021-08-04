module "member_service_staging_project" {
  source              = "../src/resource/project"
  project_id          = "cbh-member-service-staging"
  name                = "Member Service Staging"
  app_engine_location = "us-east4"
  api_services = [                      // APIs for Endpoints: https://cloud.google.com/endpoints/docs/openapi/get-started-app-engine-standard#checking_required_services
    "appengine.googleapis.com",         // app engine
    "endpoints.googleapis.com",         // app engine
    "servicecontrol.googleapis.com",    // app engine
    "servicemanagement.googleapis.com", // app engine
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "cloudbuild.googleapis.com",
    "containerregistry.googleapis.com", // enabled by cloudbuild
    "pubsub.googleapis.com",            // enabled by cloudbuid
    "storage-api.googleapis.com",       // enabled by cloudbuid
    "iam.googleapis.com",               // enabled by a dependency
    "iamcredentials.googleapis.com",    // enabled by a dependency
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "sqladmin.googleapis.com" // necessary for communicating w/ Cloud SQL instance
  ]
}
