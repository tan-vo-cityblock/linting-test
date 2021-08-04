module "quality_measure_service_staging_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-quality-measure-service-staging"
  project_id = module.services_staging_project.project_id

  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/storage.objectAdmin"
      members = [
        "serviceAccount:${module.services_postgres_instance.svc_account_email}",
      ]
    }
  ]
}
