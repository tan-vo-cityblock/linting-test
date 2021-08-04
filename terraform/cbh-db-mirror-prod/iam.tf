module "patient_ping_transfer_service_account" {
  source     = "../src/data/service_account"
  project_id = "cityblock-data"
  account_id = "patient-ping-transfer"
}

module "healthix_transfer_service_account" {
  source     = "../src/data/service_account"
  project_id = "cityblock-data"
  account_id = "healthix-transfer"
}

module "dataflow_runner_service_account" {
  source     = "../src/data/service_account"
  project_id = "cityblock-data"
  account_id = "dataflow-job-runner"
}

module "sli_metrics_service_account" {
  source     = "../src/data/service_account"
  project_id = "cbh-sre"
  account_id = "sli-metrics"
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.db_mirror_prod.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:data-team@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.patient_ping_transfer_service_account.email}",
    "serviceAccount:${module.healthix_transfer_service_account.email}",
    "serviceAccount:${module.development_dataflow_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.dbt_staging_svc_acct.email}"
  ]
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.db_mirror_prod.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.patient_ping_transfer_service_account.email}",
    "serviceAccount:${module.healthix_transfer_service_account.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.dbt_staging_svc_acct.email}"
  ]
}

module "bq_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.db_mirror_prod.project_id
  role       = "roles/bigquery.user"
  members = [
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.dbt_staging_svc_acct.email}"
  ]
}

