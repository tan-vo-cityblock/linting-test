/* Cityblock Quality Measure Service module contains all relevant resources around the Quality Measure Service API
*/

locals {
  is_staging = length(regexall("staging", var.project_id)) > 0
  is_prod    = length(regexall("prod", var.project_id)) > 0
  environ    = local.is_prod ? "prod" : "staging"
  NODE_ENV   = local.is_prod ? "production" : "staging"
}

// Connection of app engine to postgres database
resource "google_project_iam_member" "app_db_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${var.project_id}@appspot.gserviceaccount.com"
}

//IAM for cityblock-data cloudbuild service account to build app and run migrations in app engine project.

module "app_cloudbuild_iam" {
  source                = "../cloudbuild_app_engine_iam"
  app_engine_project_id = var.project_id
  grant_cloudsql_perms  = true
}

// trigger for deploying application
resource "google_cloudbuild_trigger" "deploy_quality_measure_service" {
  name        = "quality-measure-service-${local.environ}-trigger"
  description = "Deploy Quality Measure Service ${local.environ} app in project ${var.project_id}"
  project     = "cityblock-data" // trigger is in this project as the project contains the mixer repo
  trigger_template {
    project_id  = "cityblock-data"
    repo_name   = "github_cityblock_mixer" // tied to the Google Source Repository that mirrors github mixer
    branch_name = var.deployment_branch_name
    tag_name    = var.deployment_tag_name
  }
  substitutions = {
    _CLOUD_SQL_INSTANCE = var.cloud_sql_instance
    _DEPLOY_ENV = local.environ
    _NODE_ENV = local.NODE_ENV
    _APP_PROJECT = var.project_id
    _SECRETS_PROJECT = "cbh-secrets"
  }

  included_files = ["services/quality_measure/**"]
  filename       = "services/quality_measure/qm_cloudbuild.yaml"
}
