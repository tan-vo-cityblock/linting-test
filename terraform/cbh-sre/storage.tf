module "reliability_metrics_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = module.sre_project.project_id
  name          = "cbh-reliability-metrics"
  storage_class = "MULTI_REGIONAL"
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/storage.objectCreator"
      members = ["serviceAccount:${module.sli_metrics_svc_acct.email}"]
    },
    {
      role = "roles/storage.legacyBucketReader"
      members = ["serviceAccount:${module.sli_metrics_svc_acct.email}"]
    }
  ]
}
