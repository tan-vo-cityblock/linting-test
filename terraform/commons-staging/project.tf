resource "google_project" "commons-staging" {
  name            = "Commons Staging"
  project_id      = "commons-183915"
  folder_id       = "1068638637406" # Non-production
  billing_account = var.billing_account
}

// TODO: this project is missing GCS/bucket resource creation, and needs to be reflective of what is in production
