module "quality_measure_service_prod_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-quality-measure-service-prod"
  project_id = module.services_prod_project.project_id
  labels = {
    data = "phi"
  }
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
