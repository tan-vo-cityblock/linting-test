module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.sre_project.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.sli_metrics_svc_acct.email}"
  ]
}

module "bq_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.sre_project.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:data-team@cityblock.com",
    "serviceAccount:${module.looker_svc_acct.email}"
  ]
}
