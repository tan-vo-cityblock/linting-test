##  test service account
module "svc_acct_test" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-test"
  }
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "test-airflow"

  account_description = <<EOF
  Service account for running DAGs in the test cloud composer environment.
  Has read permissions to all production projects and write to a dedicated
  EOF
}

resource "google_project_iam_member" "test_read_cityblock_data_bq" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "cityblock-data"
}

resource "google_project_iam_member" "test_read_cityblock_data_gcs" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/viewer"
  project = "cityblock-data"
}

resource "google_project_iam_member" "test_read_emblem_bq" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "emblem-data"
}

resource "google_project_iam_member" "test_read_cci_bq" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "connecticare-data"
}

resource "google_project_iam_member" "test_read_ref_data_bq" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}

resource "google_project_iam_member" "test_read_orchestration" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/viewer"
  project = var.project_id
}

resource "google_project_iam_member" "test_write_orchestration_bq" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/bigquery.admin"
  project = var.project_id
}

resource "google_project_iam_member" "test_write_orchestration_gcs" {
  member  = "serviceAccount:${module.svc_acct_test.service_account_email}"
  role    = "roles/storage.admin"
  project = var.project_id
}


##  example_scio DAG
module "svc_acct_example_scio" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "example-scio-job"

  account_description = <<EOF
  Service account for running `example_scio` DAG on cloud composer environment
  project: cityblock-orchestration
  first scio: create dataflow job, access bigquery, write to gcs
  second scio: create dataflow job, write to bigquery, write to gcs
  compute service account where dataflow is run has bulk of job permissions
  EOF
}

resource "google_project_iam_member" "example_scio_bq_editor" {
  member  = "serviceAccount:${var.tanjin_compute_service_account.email}"
  role    = "roles/bigquery.dataEditor"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_bq_job_user" {
  member  = "serviceAccount:${var.tanjin_compute_service_account.email}"
  role    = "roles/bigquery.jobUser"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_bq_user" {
  member  = "serviceAccount:${var.tanjin_compute_service_account.email}"
  role    = "roles/bigquery.user"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_dataflow_worker" {
  member  = "serviceAccount:${var.tanjin_compute_service_account.email}"
  role    = "roles/dataflow.worker"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_storage_obj_admin" {
  member  = "serviceAccount:${var.tanjin_compute_service_account.email}"
  role    = "roles/storage.objectAdmin"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_dataflow" {
  member  = "serviceAccount:${module.svc_acct_example_scio.service_account_email}"
  role    = "roles/dataflow.admin"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_bq_meta" {
  member  = "serviceAccount:${module.svc_acct_example_scio.service_account_email}"
  role    = "roles/bigquery.metadataViewer"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_gcs" {
  member  = "serviceAccount:${module.svc_acct_example_scio.service_account_email}"
  role    = "roles/storage.admin"
  project = var.tanjin_compute_service_account.project
}

resource "google_project_iam_member" "example_scio_image_access" {
  member  = "serviceAccount:978071466575-compute@developer.gserviceaccount.com"
  role    = "roles/storage.objectViewer"
  project = "cbh-tanjin-panna"
}


## cityblock orchestration instance permissions
resource "google_project_iam_member" "airflow_worker_image_access" {
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
  role    = "roles/storage.objectViewer"
  project = "cityblock-data"
}


## load_monthly_data_emblem DAG
module "svc_acct_load_monthly_data_emblem" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "load-monthly-emblem"

  account_description = <<EOF
  Service account for running `load_monthly_data_emblem` DAG on cloud composer environment
  project: cityblock-orchestration
  scio_load_monthly_data: write GCS (staging Dataflow)
  get_latest_bq_table_shard: read bigquery  (emblem-data)
  scio_load_monthly_data: dataflow, bigquery access to staging, reference, and emblem
  EOF
}

resource "google_service_account_key" "load_emblem_monthly_svc_acct_key" {
  service_account_id = module.svc_acct_load_monthly_data_emblem.svc_acct_name
}

resource "google_project_iam_member" "load_monthly_data_emblem_drop_bucket" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/storage.admin"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_emblem_bq_viewer" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "emblem-data"
}

resource "google_project_iam_member" "load_monthly_data_emblem_ref_staging" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "staging-emblem-data"
}

resource "google_project_iam_member" "load_monthly_data_emblem_ref_data" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}

resource "google_project_iam_member" "load_monthly_data_emblem_bq_editor" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/bigquery.dataEditor"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_emblem_bq_job" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/bigquery.jobUser"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_emblem_dataflow" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_emblem.service_account_email}"
  role    = "roles/dataflow.admin"
  project = "cityblock-data"
}


## load_monthly_data_cci DAG
module "svc_acct_load_monthly_data_connecticare" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "load-monthly-cci"

  account_description = <<EOF
  Service account for running `load_monthly_data_cci` DAG on cloud composer environment
  project: cityblock-orchestration
  scio_load_monthly_data: write GCS (staging Dataflow)
  get_latest_bq_table_shard: read bigquery  (connecticare-data)
  scio_load_monthly_data: dataflow, bigquery access to staging, reference, and cci
  EOF
}

resource "google_project_iam_member" "load_monthly_data_connecticare_drop_bucket" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/storage.admin"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_connecticare_bq_viewer" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "connecticare-data"
}

resource "google_project_iam_member" "load_monthly_data_connecticare_bq_editor" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/bigquery.dataEditor"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_connecticare_bq_job" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/bigquery.jobUser"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_connecticare_dataflow" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/dataflow.admin"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_monthly_data_connecticare_ref_data" {
  member  = "serviceAccount:${module.svc_acct_load_monthly_data_connecticare.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}

## load_weekly_pbm_emblem DAG
module "svc_acct_load_weekly_pbm_emblem" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "load-weekly-pbm-emblem"

  account_description = <<EOF
  Service account for running `load_weekly_pbm_emblem` DAG on cloud composer environment
  project: cityblock-orchestration
  scio_load_pbm_emblem: write GCS (staging Dataflow)
  get_latest_bq_table_shard: read bigquery  (emblem-data)
  scio_load_pbm_emblem: dataflow on cityblock-data
  update_silver_views: update views in Big Query
  EOF
}

resource "google_service_account_key" "load_emblem_weekly_pbm_svc_acct_key" {
  service_account_id = module.svc_acct_load_weekly_pbm_emblem.svc_acct_name
}

resource "google_project_iam_member" "load_weekly_pbm_emblem_drop_bucket" {
  member  = "serviceAccount:${module.svc_acct_load_weekly_pbm_emblem.service_account_email}"
  role    = "roles/storage.admin"
  project = "cityblock-data"
}

resource "google_project_iam_member" "load_weekly_pbm_emblem_bq_viewer" {
  member  = "serviceAccount:${module.svc_acct_load_weekly_pbm_emblem.service_account_email}"
  role    = "roles/bigquery.dataEditor"
  project = "emblem-data"
}

resource "google_project_iam_member" "load_weekly_pbm_emblem_dataflow" {
  member  = "serviceAccount:${module.svc_acct_load_weekly_pbm_emblem.service_account_email}"
  role    = "roles/dataflow.admin"
  project = "cityblock-data"
}

## cm_delegation_reports DAG
module "svc_acct_cm_delegation_reports" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "cm-delegation-reports"

  account_description = <<EOF
  Service account for running weekly `cm_delegation_reports` DAG on cloud composer environment
  project: cityblock-orchestration
  run_cm_reports: read bigquery (cityblock-data, emblem-data, connecticare-data, ref-data, cbh-LO)
                  & write bigquery (cityblock-data), create jobs bigquery (cbh-orchestration)
  EOF
}

resource "google_project_iam_member" "cm_reports_cbh_editor" {
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.dataEditor"
  project = "cityblock-data"
}

resource "google_project_iam_member" "cm_reports_emblem_viewer" {
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "emblem-data"
}

resource "google_project_iam_member" "cm_reports_cci_viewer" {
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "connecticare-data"
}

resource "google_project_iam_member" "cm_reports_cbh_orch_job" {
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.jobUser"
  project = "cityblock-orchestration"
}

resource "google_project_iam_member" "cm_reports_ref_viewer" {
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}

resource "google_project_iam_member" "cm_reports_lo_viewer" { ## until CnU productionized
  member  = "serviceAccount:${module.svc_acct_cm_delegation_reports.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "cbh-lesli-ott"
}

# able health
module "svc_acct_able_health" {
  project_id = var.project_id
  source     = "../svc_acct_k8_deprecated"
  account_id = "able-health"

  account_description = <<EOF
  Service account used for running workflows related to Able Health, consists of BQ access to
  populate data into GCS buckets and running jobs on cityblock-data to populate data from AH
  EOF
}

resource "google_project_iam_member" "ah_gcs_bucket_admin" {
  member  = "serviceAccount:${module.svc_acct_able_health.service_account_email}"
  role    = "roles/storage.admin"
  project = var.project_id
}

resource "google_project_iam_member" "ah_gcs_bucket_object_admin" {
  member  = "serviceAccount:${module.svc_acct_able_health.service_account_email}"
  role    = "roles/storage.objectAdmin"
  project = var.project_id
}

resource "google_project_iam_member" "ah_bq_runner" {
  member  = "serviceAccount:${module.svc_acct_able_health.service_account_email}"
  role    = "roles/bigquery.user"
  project = var.project_id
}

resource "google_project_iam_member" "ah_cbh_job_user" {
  member  = "serviceAccount:${module.svc_acct_able_health.service_account_email}"
  role    = "roles/bigquery.jobUser"
  project = "cityblock-data"
}

# TODO give more fine-grainer permissions when dataset (hedis) is available on terraform
resource "google_project_iam_member" "ah_cbh_ref_data" {
  member  = "serviceAccount:${module.svc_acct_able_health.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "reference-data-199919"
}


module "elation_worker_svc_acct" {
  source = "../src/custom/svc_acct_key_k8_secret"
  project_id = var.project_id
  account_id = "elation-worker"
  account_description = <<EOF
  Service account used for running jobs/workflows related to Elation data.

  Note this account is different from prod-elation-mirror service account used exclusively for mirroring our
  Elation Hosted DB.
  EOF
}

resource "google_service_account_key" "elation_worker_svc_acct_key" {
  service_account_id = module.elation_worker_svc_acct.svc_acct_name
}

resource "google_project_iam_member" "elation_worker_cityblock_data_dataflow_admin" {
  member  = "serviceAccount:${module.elation_worker_svc_acct.email}"
  role    = "roles/dataflow.admin"
  project = module.cityblock_data_project_ref.project_id
}

resource "google_project_iam_member" "elation_worker_cityblock_data_editor" {
  member  = "serviceAccount:${module.elation_worker_svc_acct.email}"
  role    = "roles/bigquery.dataEditor"
  project = module.cityblock_data_project_ref.project_id
}

resource "google_project_iam_member" "elation_worker_cityblock_data_job_user" {
  member  = "serviceAccount:${module.elation_worker_svc_acct.email}"
  role    = "roles/bigquery.jobUser"
  project = module.cityblock_data_project_ref.project_id
}


resource "google_project_iam_member" "elation_worker_orch_job_user" {
  member  = "serviceAccount:${module.elation_worker_svc_acct.email}"
  role    = "roles/bigquery.jobUser"
  project = var.project_id
}

# patient panel management
module "svc_acct_patient_panel_management" {
  source     = "../svc_acct_k8_deprecated"
  project_id = var.project_id
  account_id = "panel-management"

  account_description = <<EOF
  Service account used for dumping cityblock-analytics abs_panel_mgmt and mrt_panel_mgmt bq datasets into GCS buckets.
  Has read access for bq in cityblock-analytics, and write access for gs://cityblock-production-patient-data.
  EOF
}

resource "google_project_iam_member" "patient_panel_management_data_reader" {
  member  = "serviceAccount:${module.svc_acct_patient_panel_management.service_account_email}"
  role    = "roles/bigquery.dataViewer"
  project = "cityblock-analytics"
}

resource "google_project_iam_member" "patient_panel_management_data_runner" {
  member  = "serviceAccount:${module.svc_acct_patient_panel_management.service_account_email}"
  role    = "roles/bigquery.user"
  project = "cityblock-analytics"
}

resource "google_project_iam_member" "patient_panel_management_file_creator" {
  member  = "serviceAccount:${module.svc_acct_patient_panel_management.service_account_email}"
  role    = "roles/storage.objectAdmin"
  project = "commons-production"
}

# qreviews
module "svc_acct_q_reviews" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "qreviews"

  account_description = <<EOF
  Service account used for retrieving data from Q-Reviews.

  BQ permissions for creating tables (if necessary) and writing new data
  EOF
}

resource "google_project_iam_member" "q_reviews_job_user" {
  member  = "serviceAccount:${module.svc_acct_q_reviews.email}"
  role    = "roles/bigquery.jobUser"
  project = "cityblock-data"
}

# patient eligibilities
module "svc_acct_patient_eligibilities" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "patient-eligibilities"

  account_description = "Service account used for updating patient eligibilities on GCS (eventually ingested by Commons)."
}

resource "google_storage_bucket_iam_member" "patient_eligibilities_storage_admin" {
  member = "serviceAccount:${module.svc_acct_patient_eligibilities.email}"
  role   = "roles/storage.objectAdmin"
  bucket = "cityblock-production-patient-data"
}

// TOOD: all partner service accounts should eventually be refactored into their own partner specific projects
module "svc_acct_load_daily_tufts" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "load-daily-tufts"

  account_description = <<EOF
  Ingests and transforms daily member eligibility files from Tufts.
  EOF
}

module "svc_acct_carefirst_worker" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "carefirst-worker"

  account_description = <<EOF
  Ingests and transforms daily member eligibility files from CareFirst.
  EOF
}

resource "google_project_iam_member" "load_carefirst_worker_ref_data" {
  member  = "serviceAccount:${module.svc_acct_carefirst_worker.email}"
  role    = "roles/bigquery.dataViewer"
  project = module.reference_data_project_ref.project_id
}

module "svc_acct_cardinal_worker" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "cardinal-worker"

  account_description = <<EOF
  Ingests and transforms files from Cardinal.
  EOF
}

resource "google_project_iam_member" "load_cardinal_worker_ref_data" {
  member  = "serviceAccount:${module.svc_acct_cardinal_worker.email}"
  role    = "roles/bigquery.dataViewer"
  project = module.reference_data_project_ref.project_id
}

module "svc_acct_healthyblue_worker" {
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "healthyblue-worker"

  account_description = <<EOF
  Ingests and transforms files from Healthy Blue.
  EOF
}

resource "google_project_iam_member" "load_healthyblue_worker_ref_data" {
  member  = "serviceAccount:${module.svc_acct_healthyblue_worker.email}"
  role    = "roles/bigquery.dataViewer"
  project = module.reference_data_project_ref.project_id
}

module "airflow_ae_firewall_update_secret" {
  source              = "../src/custom/svc_acct_key_k8_secret"
  project_id          = var.project_id
  account_id          = "airflow-ae-cf-invoker"
  account_description = "Service account with the ability to call the Cloud Function to update firewall rules of an App Engine service"
}

module "dnah_jobs_svc_acct_secret" {
  source              = "../src/custom/svc_acct_key_k8_secret"
  project_id          = var.project_id
  account_id          = "dnah-job-runner"
  account_description = "Service account for running DNAH (Days Not At Home) jobs/processes"
}

# Redox/Epic service account
module "redox_worker_svc_acct" {
  source = "../src/custom/svc_acct_key_k8_secret"
  project_id = var.project_id
  account_id = "redox-worker"
  account_description = <<EOF
  Service account used for running jobs/workflows related to Redox/Epic data.
  EOF
}

resource "google_project_iam_member" "redox_worker_cityblock_data_dataflow_admin" {
  member  = "serviceAccount:${module.redox_worker_svc_acct.email}"
  role    = "roles/dataflow.admin"
  project = module.cityblock_data_project_ref.project_id
}

resource "google_project_iam_member" "redox_worker_cityblock_data_editor" {
  member  = "serviceAccount:${module.redox_worker_svc_acct.email}"
  role    = "roles/bigquery.dataEditor"
  project = module.cityblock_data_project_ref.project_id
}

resource "google_project_iam_member" "redox_worker_cityblock_data_job_user" {
  member  = "serviceAccount:${module.redox_worker_svc_acct.email}"
  role    = "roles/bigquery.jobUser"
  project = module.cityblock_data_project_ref.project_id
}


resource "google_project_iam_member" "redox_worker_orch_job_user" {
  member  = "serviceAccount:${module.redox_worker_svc_acct.email}"
  role    = "roles/bigquery.jobUser"
  project = var.project_id
}

# Zendesk Workers

module "prod_zendesk_worker_svc_acct" {
  source = "../src/custom/svc_acct_key_k8_secret"
  project_id = var.project_id
  account_id = "prod-zendesk-worker"
  account_description = <<EOF
  Service account used for running jobs/workflows related to our prod Zendesk instance.
  EOF
}

module "staging_zendesk_worker_svc_acct" {
  source = "../src/custom/svc_acct_key_k8_secret"
  project_id = var.project_id
  account_id = "staging-zendesk-worker"
  account_description = <<EOF
  Service account used for running jobs/workflows related to our prod Zendesk instance.
  EOF
}
