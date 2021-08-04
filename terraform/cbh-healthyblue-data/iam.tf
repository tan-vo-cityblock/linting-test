module "bq_data_editors" {
  source     = "../src/custom/project_iam_access"
  project_id = module.healthyblue_project.project_id
  role       = "roles/bigquery.dataEditor"
  members = [
    "group:data-team@cityblock.com",
    "serviceAccount:${module.svc_acct_healthyblue_worker.email}"
  ]
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.healthyblue_project.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.development_dataflow_svc_acct.email}"
  ]
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.healthyblue_project.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:97093835752-compute@developer.gserviceaccount.com"
  ]
}
