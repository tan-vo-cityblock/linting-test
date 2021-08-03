/* Cityblock Member Service module contains all relevant resources around the Member Service API
*/

locals {
  is_staging = length(regexall("staging", var.project_id)) > 0
  is_prod    = length(regexall("prod", var.project_id)) > 0
  environ    = local.is_prod ? "prod" : "staging"
  NODE_ENV   = local.is_prod ? "production" : "staging"
}

// Shared VPC project
data "terraform_remote_state" "cbh_shared_vpc_ref" {
  backend = "gcs"
  config = {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-shared-vpc-${local.environ}"
  }
}

// Connection of app engine to postgres database
resource "google_project_iam_member" "app_db_client" {
  project = var.database_project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${var.project_id}@appspot.gserviceaccount.com"
}

//IAM for cityblock-data cloudbuild service account to build app and run migrations in app engine project.

module "app_cloudbuild_iam" {
  source                = "../cloudbuild_app_engine_iam"
  app_engine_project_id = var.project_id
  grant_cloudsql_perms  = true
  cloudsql_project_id   = var.database_project
}

// trigger for deploying application
resource "google_cloudbuild_trigger" "deploy_member_service" {
  description = "Deploy Member Service application for ${var.project_id}"
  project     = "cityblock-data" // trigger is in this project as the project contains the mixer repo
  trigger_template {
    project_id  = "cityblock-data"
    repo_name   = "github_cityblock_mixer" // tied to the Google Source Repostiry that mirrors github mixer
    dir         = "services/member_service"
    branch_name = var.deployment_branch_name
    tag_name    = var.deployment_tag_name
  }

  substitutions = {
    _CLOUD_SQL_INSTANCE = var.cloud_sql_instance
    _DEPLOY_ENV = local.environ
    _NODE_ENV = local.NODE_ENV
    _APP_PROJECT = var.project_id
    _SECRETS_PROJECT = "cbh-secrets"
    _DB_NAME = "member"  # TODO change to local.environ if Cloud Run does not work out
    _MEMBER_SERVICE_IMAGE = "us.gcr.io/cityblock-data/member-service-${local.environ}"
  }

  included_files = ["services/member_service/**"]
  filename       = "services/member_service/member_service_cloudbuild.yaml"
}

//App engine firewall rules
resource "google_app_engine_firewall_rule" "blacklist" {
  project      = var.project_id
  priority     = 2147483646
  action       = "DENY"
  source_range = "*"
}

resource "google_app_engine_firewall_rule" "commons_prod_1" {
  project      = var.project_id
  priority     = 1
  action       = local.is_prod ? "ALLOW" : "DENY"
  source_range = "34.234.191.214"
}

resource "google_app_engine_firewall_rule" "commons_prod_2" {
  project      = var.project_id
  priority     = 2
  action       = local.is_prod ? "ALLOW" : "DENY"
  source_range = "54.85.156.227"
}

resource "google_app_engine_firewall_rule" "gcp_us_central1" {
  project      = var.project_id
  priority     = 3
  action       = local.is_prod || local.is_staging ? "ALLOW" : "DENY"
  source_range = "10.128.0.0/20"
}

resource "google_app_engine_firewall_rule" "cbh_dumbo" {
  project      = var.project_id
  priority     = 4
  action       = local.is_staging || local.is_prod ? "ALLOW" : "DENY"
  source_range = "68.160.220.179"
}

resource "google_app_engine_firewall_rule" "commons_staging_1" {
  project      = var.project_id
  priority     = 5
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "52.73.142.147"
}


resource "google_app_engine_firewall_rule" "commons_staging_2" {
  project      = var.project_id
  priority     = 6
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "52.45.148.202"
}


resource "google_app_engine_firewall_rule" "commons_staging_3" {
  project      = var.project_id
  priority     = 7
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "3.213.230.147"
}

resource "google_app_engine_firewall_rule" "app_engine_cron" {
  project      = var.project_id
  priority     = 8
  action       = local.is_staging || local.is_prod ? "ALLOW" : "DENY"
  source_range = "0.1.0.1"
}
