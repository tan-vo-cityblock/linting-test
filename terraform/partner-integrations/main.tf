/* Partner Integrations project contains all relevant resources
around the BigQuery analyses for data during early stages of partner data investigation */

provider "google" {
  version = "~> 3.44.0"
}

resource "google_project" "partner_integrations" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing
}

resource "google_project_iam_member" "owner" {
  project = google_project.partner_integrations.project_id
  member  = "group:eng-all@cityblock.com"
  role    = "roles/owner"
}

resource "google_project_iam_member" "product-lead" {
  project = google_project.partner_integrations.project_id
  member  = "user:jordan.hartfiel@cityblock.com"
  role    = "roles/editor"
}

resource "google_project_service" "partner_integrations_bq" {
  project                    = google_project.partner_integrations.project_id
  service                    = "bigquery.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/partner_integrations"
  }
}
