/* Cityblock Actuary project contains all relevant resources
around the BigQuery analyses and GCS artifacts built and maintained by
Actuary */

provider google {
  version = "~> 3.44.0"
}

resource "google_project" "actuary" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing
}

resource "google_project_service" "actuary_bq" {
  project                    = google_project.actuary.project_id
  service                    = "bigquery.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_storage_bucket" "scratch_bucket" {
  location = "US"
  name     = "cbh-actuary-scratch"
  project  = google_project.actuary.id
}

terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/actuary"
  }
}
