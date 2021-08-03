module "temp_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = module.development_project.project_id
  name          = "cbh-dev-temp"
  storage_class = "MULTI_REGIONAL"
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com",
        "serviceAccount:${module.dataflow_svc_acct.email}"
      ]
    }
  ]
}
