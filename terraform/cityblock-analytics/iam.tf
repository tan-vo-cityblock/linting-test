module "project_owners" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/owner"
  members = [
    "group:data-team@cityblock.com",
    "group:eng-all@cityblock.com"
  ]
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:looker-demo@cityblock-data.iam.gserviceaccount.com",
    "serviceAccount:${module.dnah_jobs_svc_acct.email}",
    "serviceAccount:${module.sli_metrics_service_account.email}",
    "serviceAccount:${module.cityblock_dev_svc_acct.email}",
    "serviceAccount:${module.commons_prod_svc_acct.email}",
    "serviceAccount:${module.commons_staging_svc_acct.email}",
    "serviceAccount:${module.able_health_worker_svc_acct.email}"
  ]
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "group:actuary@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:looker-demo@cityblock-data.iam.gserviceaccount.com",
    "serviceAccount:dataflow-job-runner@cityblock-data.iam.gserviceaccount.com",
    "serviceAccount:${module.dnah_jobs_svc_acct.email}",
    "serviceAccount:${module.ml_labeling_svc_acct.email}",
    "serviceAccount:${module.tf_svc_load_monthly_cci_ref.email}",
    "serviceAccount:${module.load_emblem_service_account.email}",
    "serviceAccount:${module.load_tufts_service_account.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  ]
}

module "bq_data_editors" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/bigquery.dataEditor"
  members = [
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.ml_labeling_svc_acct.email}"
  ]
}

module "bq_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/bigquery.user"
  members = [
    "serviceAccount:${module.dbt_prod_svc_acct.email}"
  ]
}

module "iap_data_docs_access" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cbh_analytics_project.project_id
  role       = "roles/iap.httpsResourceAccessor"
  members = [
    "group:actuary@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "group:data-team@cityblock.com",
    "group:eng-all@cityblock.com",
    "group:market-ops-ct@cityblock.com",
    "group:market-ops-dc@cityblock.com",
    "group:market-ops-ma@cityblock.com",
    "group:market-ops-ny@cityblock.com",
    "group:market-ops@cityblock.com",
    "group:pmux@cityblock.com"
  ]
}
