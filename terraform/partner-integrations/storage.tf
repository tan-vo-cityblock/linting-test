module "virta_health_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = google_project.partner_integrations.project_id
  name          = "cbh-virta-health"
  storage_class = "MULTI_REGIONAL"
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com"
      ]
    },
    {
      role = "roles/storage.objectAdmin"
      members = [
        "user:michael.ruan@cityblock.com",
        "serviceAccount:partner-access-city-block@virta-eng-prod.iam.gserviceaccount.com"
      ]
    }
  ]
}
