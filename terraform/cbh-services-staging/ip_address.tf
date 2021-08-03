resource "google_compute_global_address" "member_service_ip" {
  project = module.services_staging_project.project_id
  name    = "member-service-staging"
}
