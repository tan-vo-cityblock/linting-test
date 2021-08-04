variable "billing_account" {
  type        = "string"
  description = "Billing account to use for personal project"
}

variable "folder_id" {
  type        = "string"
  description = "Folder to put personal project in"
}

variable "person_name" {
  type        = "string"
  description = "Name of person project is being created for"
}

variable "person_email" {
  type        = "string"
  description = "Email of person project is being created for"
}

resource "google_project" "personal_project" {
  name       = "${var.person_name}'s Sandbox"
  project_id = "cbh-${lower(replace(var.person_name, " ", "-"))}"

  billing_account = "${var.billing_account}"
  folder_id       = "${var.folder_id}"
}

resource "google_project_service" "personal_project_bigquery" {
  project                    = "${google_project.personal_project.project_id}"
  service                    = "bigquery.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_iam_member" "project_owner" {
  project = "${google_project.personal_project.project_id}"
  role    = "roles/owner"
  member  = "user:${var.person_email}"
}

resource "google_storage_bucket" "scratch_bucket" {
  location = "US"
  name     = "cbh-${lower(replace(var.person_name, " ", "-"))}-scratch"
  project  = "${google_project.personal_project.project_id}"
}

output "personal_project_id" {
  value = "${google_project.personal_project.project_id}"
}

output "project_url" {
  value = "https://console.cloud.google.com/home/dashboard?project=${google_project.personal_project.project_id}"
}

output "scratch_bucket" {
  value = google_storage_bucket.scratch_bucket
}
