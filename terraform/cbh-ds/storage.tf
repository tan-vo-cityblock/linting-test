module "ds_interface_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = module.data_science_project.project_id
  name          = "cbh-ds-interface"
  storage_class = "MULTI_REGIONAL"
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com",
        "group:data-science@cityblock.com"
      ]
    },
    {
      role = "roles/storage.objectAdmin"
      members = [
        "serviceAccount:${module.cotiviti_svc_acct.email}"
      ]
    }
  ]
}
