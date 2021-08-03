resource "google_project_iam_member" "owner" {
  project = var.project_id
  role    = "roles/owner"
  member  = "group:eng-all@cityblock.com"
}

resource "google_project_iam_member" "compute-engine" {
  project = var.project_id
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
  role    = "roles/editor"
}

resource "google_project_iam_member" "data-team-editors" {
  project = var.project_id
  role    = "roles/editor"
  member  = "group:data-team@cityblock.com"
}

data "google_compute_default_service_account" "default" {
  project = var.project_id
}

module "orchestration_bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = var.project_id
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}",
    "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}",
    // TODO: do we need this?
    "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  ]
}

module "dataflow_callers" {
  source     = "../src/custom/project_iam_access"
  project_id = module.cityblock_data_project_ref.project_id  // All dataflow jobs are run in this project
  role       = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${module.svc_acct_example_scio.service_account_email}",
    "serviceAccount:${module.elation_worker_svc_acct.email}",
    "serviceAccount:${module.redox_worker_svc_acct.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}",
    "serviceAccount:${module.svc_acct_load_daily_tufts.email}",
    "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}",
    "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}",
    "serviceAccount:${module.svc_acct_payer_suspect_service.email}",
    "serviceAccount:${module.svc_acct_load_weekly_pbm_emblem.service_account_email}",
    "serviceAccount:${module.svc_acct_patient_eligibilities.email}",
    "serviceAccount:${module.svc_acct_cardinal_worker.email}",
    "serviceAccount:${module.svc_acct_healthyblue_worker.email}",
  ]
}

module "svc_acct_token_creators" {
  source     = "../src/custom/project_iam_access"
  project_id = var.project_id
  role       = "roles/iam.serviceAccountTokenCreator"
  members = [
    "serviceAccount:${module.load_gcs_data_cf_svc_acct.email}"
  ]
}

module "composer_users" {
  source     = "../src/custom/project_iam_access"
  project_id = var.project_id
  role       = "roles/composer.user"
  members = [
    "serviceAccount:${module.load_gcs_data_cf_svc_acct.email}"
  ]
}
