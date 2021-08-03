locals {
  is_staging = length(regexall("staging", var.project_id)) > 0
  is_prod    = length(regexall("prod", var.project_id)) > 0
  environ    = local.is_prod ? "prod" : "staging"
  NODE_ENV   = local.is_prod ? "production" : "staging"
}

resource "google_cloudbuild_trigger" "deploy_health_events_service" {
  description = "Deploy Health Events Service application for ${var.project_id}"
  project     = "cityblock-data" // trigger is in this project as the project contains the mixer repo
  trigger_template {
    project_id  = "cityblock-data"
    repo_name   = "github_cityblock_mixer" // tied to the Google Source Repostiry that mirrors github mixer
    dir         = "services/health_events_service"
    branch_name = var.deployment_branch_name
    tag_name    = var.deployment_tag_name
  }

  substitutions = {
    _CLOUD_SQL_INSTANCE = var.cloud_sql_instance
    _DEPLOY_ENV = local.environ
    _NODE_ENV = local.NODE_ENV
    _APP_PROJECT = var.project_id
    _HEALTH_EVENTS_SERVICE_IMAGE = "us.gcr.io/cityblock-data/health-events-${local.environ}"
    _SECRETS_PROJECT = "cbh-secrets"
    _DB_NAME = var.database_name
  }

  included_files = ["services/health_events_service/**"]
  filename       = local.is_prod ? "services/health_events_service/cloudbuild-deployment.yaml" : "services/health_events_service/cloudbuild.yaml"
}

# the account that's running the CloudRun App
module "health_events_svc_account" {
  source = "../../resource/service_account"
  project_id = var.project_id
  account_id = "health-events-service"
}
