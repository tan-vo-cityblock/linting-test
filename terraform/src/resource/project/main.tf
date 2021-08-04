output "project_id" {
  value = google_project.project.project_id
}

output "project_number" {
  value       = google_project.project.number
  description = "The numeric identifier of the project. Computed attribute that can't be set"
}

output "default_us_gcr_bucket" {
  value = "us.artifacts.${google_project.project.project_id}.appspot.com"
}

output "default_pubsub_service_account_email" {
  value = "service-${google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

output "default_app_engine_service_account_email" {
  value       = data.google_app_engine_default_service_account.app_engine_svc_acct.email
  description = "Default app engine service account email"
}

output "default_app_engine_service_account_name" {
  value       = data.google_app_engine_default_service_account.app_engine_svc_acct.name
  description = "Default app engine service account name"
}

variable "name" {
  type        = "string"
  description = "Name of bucket"
}

variable "project_id" {
  type        = "string"
  description = "Project where bucket will be created"
}

variable "project_owner" {
  type        = "string"
  description = "Owner of the Google Project (highest level permissions)"
  default     = "group:eng-all@cityblock.com"
}

variable "billing" {
  type        = "string"
  description = "Google Cloud Billing account"
  default     = "017D3B-5DF7F9-201434"
}

variable "api_services" { // TODO: Define default list of API services and concat (remove dups) with user provided list.
  type        = list(string)
  description = "List of API services to be enabled in the project. Pass an empty list '[]' if you don't want APIs enabled"
}

variable "app_engine_location" {
  type        = "string"
  description = "The location to serve the app engine app from. Note this is optional and only needs to be set if you plan to use app engine in your project"
  default     = null
}

provider "google" {
  version = "~> 3.44.0"
}

provider "google-beta" {
  version = "~> 3.44.0"
}

resource "google_project" "project" {
  name            = var.name
  project_id      = var.project_id
  billing_account = var.billing
}

resource "google_project_service" "service" {
  for_each = toset(var.api_services)

  service = each.key

  project                    = var.project_id
  disable_on_destroy         = false
  disable_dependent_services = false
  depends_on                 = [google_project.project]
}

resource "google_app_engine_application" "app_engine" {
  count       = var.app_engine_location == null ? 0 : 1 // If location isn't set, the app_engine resource will not be created.
  project     = var.project_id
  location_id = var.app_engine_location == null ? "us-east4" : var.app_engine_location
  depends_on  = [google_project.project, google_project_service.service]
}

resource "google_project_iam_member" "project_owner" { //TODO: Use iam_policy to make more authorative when google groups for IAM is implemented.
  project    = var.project_id
  member     = var.project_owner
  role       = "roles/owner"
  depends_on = [google_project.project, google_project_service.service]
}

data "google_app_engine_default_service_account" "app_engine_svc_acct" {
  project = google_project.project.project_id
}
