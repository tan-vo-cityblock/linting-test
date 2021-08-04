resource "google_project_service" "servicenetworking" {
  project = "${var.partner_project_production}"
  service = "servicenetworking.googleapis.com"

  # We don't track all apis in terraform yet, so we don't want to
  # accidentally disable a service that some other resource depends on
  disable_dependent_services = false
}

