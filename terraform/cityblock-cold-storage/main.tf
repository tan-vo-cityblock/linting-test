/*
Cityblock Cold Storage project creates buckets for cold storage
*/

resource "google_project" "cold_storage" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing_account
}

resource "google_project_iam_member" "project_owner" {
  project = google_project.cold_storage.project_id
  role    = "roles/owner"
  member  = "group:eng-all@cityblock.com"
}
