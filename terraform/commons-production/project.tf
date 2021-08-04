output "commons_service_account_email" {
  value = "${google_service_account.commons-production.email}"
}

resource "google_project" "commons-production" {
  name            = "Commons Production"
  project_id      = "commons-production"
  folder_id       = "286685040564" # Production-PHI
  billing_account = "${var.billing_account}"
}
