module "silver_claims_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "silver_claims"
  project    = module.healthyblue_project.project_id
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com", role = "READER" },
    { email = module.svc_acct_healthyblue_worker.email, role = "READER" },
    { email = module.dbt_staging_svc_acct.email, role = "WRITER" },
    { email = module.dbt_prod_svc_acct.email, role = "WRITER" },
    { email = module.cityblock_data_project_ref.default_compute_engine_service_account_email, role = "WRITER" },
    { email = module.looker_svc_acct.email, role = "READER" }
  ]
  description = "Dataset for housing prod silver claims for Healthy Blue."
}

module "gold_claims_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "gold_claims"
  project    = module.healthyblue_project.project_id
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com", role = "READER" },
    { email = module.svc_acct_healthyblue_worker.email, role = "WRITER" },
    { email = module.dbt_staging_svc_acct.email, role = "WRITER" },
    { email = module.dbt_prod_svc_acct.email, role = "WRITER" },
    { email = module.cityblock_data_project_ref.default_compute_engine_service_account_email, role = "WRITER" },
    { email = module.looker_svc_acct.email, role = "READER" }
  ]
  description = "Dataset for housing production gold claims for Healthy Blue."
}
