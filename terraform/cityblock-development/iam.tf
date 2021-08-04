module "bq_admins" {
  source     = "../src/custom/project_iam_access"
  project_id = module.development_project.project_id
  role       = "roles/bigquery.admin"
  members = [
    "serviceAccount:${module.dataflow_svc_acct.email}"
  ]
}

module "dataflow_admins" {
  source     = "../src/custom/project_iam_access"
  project_id = module.development_project.project_id
  role       = "roles/dataflow.admin"
  members = [
    "serviceAccount:${module.dataflow_svc_acct.email}"
  ]
}

module "dataflow_workers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.development_project.project_id
  role       = "roles/dataflow.worker"
  members = [
    "serviceAccount:${module.dataflow_svc_acct.email}"
  ]
}

module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = module.development_project.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.dev_svc_acct.email}"
  ]
}
