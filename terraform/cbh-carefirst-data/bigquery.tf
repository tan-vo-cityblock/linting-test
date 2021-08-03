module "silver_claims_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "silver_claims"
  project    = module.carefirst_project.project_id
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
    { email = module.svc_acct_carefirst_worker.email, role = "READER" },
    { email = module.dbt_staging_svc_acct.email, role = "WRITER" },
    { email = module.dbt_prod_svc_acct.email, role = "WRITER" },
    { email = module.cityblock_data_project_ref.default_compute_engine_service_account_email, role = "WRITER" }
  ]
  description = "Dataset for housing prod silver claims for CareFirst."
}

module "gold_claims_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "gold_claims"
  project    = module.carefirst_project.project_id
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
    { email = module.svc_acct_carefirst_worker.email, role = "READER" },
    { email = module.dbt_staging_svc_acct.email, role = "WRITER" },
    { email = module.dbt_prod_svc_acct.email, role = "WRITER" },
    { email = module.cityblock_data_project_ref.default_compute_engine_service_account_email, role = "WRITER" },
    { email = module.able_health_svc_acct.email, role = "READER" }
  ]
  description = "Dataset for housing prod gold claims for CareFirst."
}

module "gold_claims_incremental_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "gold_claims_incremental"
  project    = module.carefirst_project.project_id
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
    { email = module.svc_acct_carefirst_worker.email, role = "READER" },
    { email = module.dbt_staging_svc_acct.email, role = "WRITER" },
    { email = module.dbt_prod_svc_acct.email, role = "WRITER" },
    { email = module.cityblock_data_project_ref.default_compute_engine_service_account_email, role = "WRITER" }
  ]
  description = "Dataset for housing prod gold claims incremental for CareFirst."
}
