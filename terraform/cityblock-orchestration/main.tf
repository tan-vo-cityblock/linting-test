/*
Cityblock Orchestration project contains all relevant resources for job orchestration

This includes:
  - Cloud Composer Environment: https://cloud.google.com/composer/

*/

provider "kubernetes" {
  alias                  = "cityblock-orchestration-test"
  version                = "~> 1.6.2"
  config_context_cluster = "gke_cityblock-orchestration_us-east4-a_us-east4-cityblock-composer-cdc9ecc8-gke"
}

resource "google_project" "orchestration" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing
}

resource "google_project_service" "cloud_composer_api" {
  project = google_project.orchestration.project_id
  service = "composer.googleapis.com"
}

resource "google_composer_environment" "prod_cluster" {
  name    = "prod-airflow"
  region  = var.default_region
  project = google_project.orchestration.project_id
  config {
    node_count = 3
    node_config {
      zone         = "${var.default_region}-a"
      disk_size_gb = 100
      machine_type = "n2-highmem-4"
    }
    software_config {
      image_version  = var.composer_version
      python_version = var.python_version
      pypi_packages = {
        numpy = "==1.16.3"
        xlrd  = "==1.2.0"
        pypd  = "==1.1.0"
      }
      airflow_config_overrides = {
        "webserver-hide_paused_dags_by_default" = "True"
        "api-auth_backend" = "airflow.api.auth.backend.default"
      }
      env_variables = {
        SENDGRID_MAIL_FROM = var.from_email
        SENDGRID_API_KEY   = var.sendgrid_api_key
      }
    }
  }
  depends_on = [google_project_service.cloud_composer_api]
}

resource "google_composer_environment" "test_cluster" {
  provider = "google-beta"
  name     = "cityblock-composer-env-test"
  region   = var.default_region
  project  = google_project.orchestration.project_id
  config {
    /** Terraform/GCP doesn't respect node pool changes to Cloud Composer environment, see note in:
    https://cloud.google.com/composer/docs/how-to/managing/updating#upgrading_the_machine_type_for_gke_nodes

    Actual details:
    node_count = 1
    disk_size = 100 GB
    machine_type = e2-highmem-2 (2 CPUs and 16 GB of memory)
    **/
    node_count = 3
    node_config {
      zone         = "${var.default_region}-a"
      disk_size_gb = 30
      machine_type = "n1-standard-1"
    }
    software_config {
      image_version  = var.composer_version
      python_version = var.python_version
      pypi_packages = {
        numpy = "==1.16.3"
        xlrd  = "==1.2.0"
        pypd  = "==1.1.0"
      }
      airflow_config_overrides = {
        "webserver-navbar_color" = "#bd6800" # orange header
        "api-auth_backend" = "airflow.api.auth.backend.default"
      }
      env_variables = {
        SENDGRID_MAIL_FROM = var.test_email
        SENDGRID_API_KEY   = var.sendgrid_api_key
      }
    }
  }
  depends_on = [google_project_service.cloud_composer_api]
}
