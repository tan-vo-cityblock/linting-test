locals {
  airflow_prod_bucket = substr(dirname(google_composer_environment.prod_cluster.config.0.dag_gcs_prefix), 4, -1)
  airflow_test_bucket = substr(dirname(google_composer_environment.test_cluster.config.0.dag_gcs_prefix), 4, -1)
  svc_account_id      = "process-payer-suspect"
}

module "svc_acct_payer_suspect_service" {
  source     = "../src/custom/svc_acct_key_k8_secret"
  project_id = var.project_id
  account_id = local.svc_account_id

  account_description = <<EOF
  Service account used for dumping payer suspect data from flat files in GCS to BQ
  EOF
}

module "airflow_conn_suspect_service" {
  source               = "../src/custom/secret_airflow_conn"
  service_account_id   = local.svc_account_id
  service_account_name = module.svc_acct_payer_suspect_service.svc_acct_name
  storage_bucket       = local.airflow_prod_bucket
}

resource "google_storage_bucket_iam_member" "payer_suspect_coding_temp_file_creator_deleter_prod" {
  bucket = local.airflow_prod_bucket
  member = "serviceAccount:${module.svc_acct_payer_suspect_service.email}"
  role   = "roles/storage.objectAdmin"
}

resource "google_storage_bucket_iam_member" "payer_suspect_coding_temp_file_creator_deleter_test" {
  bucket = local.airflow_test_bucket
  member = "serviceAccount:${module.svc_acct_payer_suspect_service.email}"
  role   = "roles/storage.objectAdmin"
}

resource "google_storage_bucket_iam_member" "elation_worker_temp_file_creator_deleter_prod" {
  bucket = local.airflow_prod_bucket
  member = "serviceAccount:${module.elation_worker_svc_acct.email}"
  role   = "roles/storage.objectAdmin"
}
