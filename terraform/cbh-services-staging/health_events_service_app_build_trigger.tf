module "health_events_cloudbuild_trigger" {
  source = "../src/custom/health-events-service"
  project_id = module.services_staging_project.project_id
  deployment_branch_name = "^master$"
  cloud_sql_instance     = module.services_postgres_instance.connection_name
  database_name          = module.health_events_db.name
}

// only relevant in staging
resource "google_cloudbuild_trigger" "pr_health_events_service" {
  provider    = google-beta
  description = "Pull request checks for Health Events Service"
  project     = "cityblock-data"
  github {
    owner = "cityblock"
    name = "mixer"
    pull_request {
      branch = "^master$"
    }
  }

  substitutions = {
    _NODE_ENV = "test"
    _CLOUD_SQL_INSTANCE = "cbh-services-staging:us-east1:services"
    _DEPLOY_ENV = "staging" 
    _SECRETS_PROJECT = "cbh-secrets"
    _DB_NAME = "health_events_service_test"
  }
  
  included_files = ["services/health_events_service/**"]
  filename       = "services/health_events_service/cloudbuild_pull_request.yaml"
}
