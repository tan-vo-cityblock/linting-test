resource "google_logging_project_exclusion" "aptible_staging_ephi" {
  project     = google_project.commons-production.project_id
  name        = "aptible-prod"
  description = "Exclude logs sent to GCP via HTTPS Log Drain on Aptible (ePHI)"
  filter      = <<EOT
resource.type="global"
labels.source="aptible"
  EOT
}
