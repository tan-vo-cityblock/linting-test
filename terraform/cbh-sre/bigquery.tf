module "sli_metrics_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "sli_metrics"
  project    = module.sre_project.project_id
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    {
      email = module.sli_metrics_svc_acct.email,
      role = "WRITER"
    }
  ]
  description = "Dataset containing all SLI metrics"
}
