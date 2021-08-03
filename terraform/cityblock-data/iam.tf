resource "google_project_iam_member" "project_owner" {
  project = var.partner_project_production
  member  = "group:eng-all@cityblock.com"
  role    = "roles/owner"
}

resource "google_project_iam_member" "cloudbuild_svc_acct_cloudfxn_dev" {
  project = "cityblock-data"
  role    = "roles/cloudfunctions.developer"
  member  = "serviceAccount:97093835752@cloudbuild.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "elation_hook_svc_pubsub_pub" {
  topic   = module.elation_events.name
  project = "cityblock-data"
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.elation_hook_svc_acct.email}"
}

resource "google_service_account_iam_member" "elation_hook_svc_acct_user" {
  service_account_id = google_service_account.elation_hook_svc_acct.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:97093835752@cloudbuild.gserviceaccount.com"
}

resource "google_project_iam_binding" "pubsub_token_creator" {
  project = module.cityblock_data_project_ref.project_id
  role = "roles/iam.serviceAccountTokenCreator"
  members = [
    "serviceAccount:${module.cityblock_data_project_ref.default_pubsub_service_account_email}",
  ]
}

resource "google_project_iam_binding" "cloud_function_invoker" {
  project = module.cityblock_data_project_ref.project_id
  role = "roles/cloudfunctions.invoker"
  members = [
    "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
    "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}",
    "serviceAccount:${module.dev_cureatr_worker_svc_acct.email}",
    "serviceAccount:${module.prod_healthgorilla_worker_svc_acct.email}",
    "serviceAccount:${module.dev_healthgorilla_worker_svc_acct.email}"
  ]
}

resource "google_project_iam_binding" "cloud_scheduler_admin" {
  project = module.cityblock_data_project_ref.project_id
  role = "roles/cloudscheduler.admin"
  members = [
    "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}",
  ]
}

resource "google_service_account_iam_member" "dataflow_runner_user" {
  service_account_id = module.cityblock_data_project_ref.default_dataflow_job_runner_id
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
}

resource "google_project_iam_member" "commons_mirror_bquser" {
  project = "cityblock-data"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.commons_prod_account}"
}


resource "google_project_iam_member" "cloudbuild_viewer_data_team" {
  project = "cityblock-data"
  role    = "roles/cloudbuild.builds.viewer"
  member  = "group:data-team@cityblock.com"
}

resource "google_project_iam_member" "danny-project-viewer" {
  member  = "user:danny.dvinov@cityblock.com"
  role    = "roles/viewer"
  project = "cityblock-data"
}

resource "google_project_iam_member" "patient-index-editor-cloudsql-client" {
  project = var.partner_project_production
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:patient-index-editor@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "danny-cloudsql-client" {
  project = var.partner_project_production
  role    = "roles/cloudsql.client"
  member  = "user:danny.dvinov@cityblock.com"
}

resource "google_project_iam_member" "cityblock-data-gce-svc-acct-access-meredith-bq" {
  project = "cbh-meredith-welch"
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:97093835752-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "katie-logs-viewer" {
  project = var.partner_project_production
  role    = "roles/logging.viewer"
  member  = "user:katie.claiborne@cityblock.com"
}

resource "google_project_iam_member" "load-daily-tufts-bigquery-viewer" {
  project = "cityblock-data"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${module.load_tufts_daily_service_account.email}"
}

resource "google_project_iam_member" "load-daily-tufts-dataflow-admin" {
  project = "cityblock-data"
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${module.load_tufts_daily_service_account.email}"
}

module "storage_admin_access" {
  source     = "../src/custom/project_iam_access"
  project_id = var.partner_project_production
  role       = "roles/storage.admin"
  members = [
    "serviceAccount:${module.load_tufts_daily_service_account.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  ]
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = var.partner_project_production
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:data-team@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:looker-demo@cityblock-data.iam.gserviceaccount.com",
    "serviceAccount:${module.dnah_jobs_svc_acct.email}",
    "serviceAccount:${module.sli_metrics_service_account.email}",
    "serviceAccount:${module.ml_labeling_svc_acct.email}",
    "serviceAccount:${module.able_health_worker_svc_acct.email}"
  ]
}

// TODO: PLAT-1348 Consolidate project level IAM into project_iam_access modules
module "bq_job_users" {
  source     = "../src/custom/project_iam_access"
  project_id = var.partner_project_production
  role       = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}",
  ]
}

module "bq_data_editors" {
  source     = "../src/custom/project_iam_access"
  project_id = var.partner_project_production
  role       = "roles/bigquery.dataEditor"
  members = [
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.load_tufts_daily_service_account.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}",
    "serviceAccount:${module.pubsub_bq_saver_cf_svc_acct.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}",
    "serviceAccount:${module.svc_acct_cardinal_worker.email}",
    "serviceAccount:${module.svc_acct_healthyblue_worker.email}"
  ]
}

module "reference_data_bq_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = "reference-data-199919"
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:data-team@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.development_dataflow_svc_acct.email}",
    "serviceAccount:${module.load_tufts_daily_service_account.email}",
  ]
}

module "reference_data_owners" {
  source     = "../src/custom/project_iam_access"
  project_id = "reference-data-199919"
  role       = "roles/owner"
  members = [
    "group:gcp-admins@cityblock.com"
  ]
}

resource "google_cloudfunctions_function_iam_member" "airflow_ae_invoker" {
  project        = google_cloudfunctions_function.airflow_ae_firewall_allow.project
  region         = google_cloudfunctions_function.airflow_ae_firewall_allow.region
  cloud_function = google_cloudfunctions_function.airflow_ae_firewall_allow.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${var.airflow_ae_cf_invoker_svc_acct}"
}

resource "google_cloudfunctions_function_iam_member" "slack_app_access" {
  project        = google_cloudfunctions_function.slack_platform_service.project
  region         = google_cloudfunctions_function.slack_platform_service.region
  cloud_function = google_cloudfunctions_function.slack_platform_service.name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}

module "compute_viewers" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/compute.viewer"
  members = [
    "serviceAccount:${module.datadog_metrics_svc_acct.email}"
  ]
}

module "monitoring_viewers" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/monitoring.viewer"
  members = [
    "serviceAccount:${module.datadog_metrics_svc_acct.email}"
  ]
}

module "asset_viewers" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/cloudasset.viewer"
  members = [
    "serviceAccount:${module.datadog_metrics_svc_acct.email}"
  ]
}

// TODO: we should move away from cf referencing "cloud function"
module "cf_bigquery_job" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${module.pubsub_bq_saver_cf_svc_acct.email}",
    "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  ]
}

module "cf_from_topic" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/pubsub.subscriber"
  members = [
    "serviceAccount:${module.pubsub_bq_saver_cf_svc_acct.email}"
  ]
}


module "dataflow_admins" {
  project_id = var.partner_project_production
  source = "../src/custom/project_iam_access"
  role = "roles/dataflow.admin"
  members = [
    "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  ]
}


