// create export gcs bucket and bq dataset
module "db_export_to_bq" {
  source                           = "../db_export_to_bq"
  environment                      = var.environment
  project_id                       = var.project_id
  database_name                    = var.database_name
  gcs_bucket_name                  = var.gcs_bucket_name
  gcs_bucket_contents_time_to_live = var.gcs_bucket_contents_time_to_live
  bq_dataset_id                    = var.bq_dataset_id
  bucket_policy_data = [
    {
      role    = "roles/storage.legacyBucketWriter",
      members = ["serviceAccount:${var.cloud_sql_instance_svc_acct}"]
    }
  ]
  bq_dataset_readers = var.bq_dataset_readers
  labels             = var.labels
}

//load cloudsql db login creds into k8
module "cbh_db_mirror_user_base64_secret_object" {
  source        = "../storage_object"
  object_bucket = "${var.db_login_kms_ring_name}-secrets"
  object_path   = "${var.db_login_kms_key_name}/${var.db_login_filename_enc}"
}

data "google_kms_secret" "cbh_db_mirror_user_secret" {
  crypto_key = var.db_login_kms_self_link
  ciphertext = module.cbh_db_mirror_user_base64_secret_object.object
}

module "cbh_db_mirror_user_creds_k8" {
  source      = "../../resource/kubernetes"
  secret_name = "cbh-db-mirror-${var.database_name}-${var.environment}-secrets"
  secret_data = {
    mirror-user-name : jsondecode(data.google_kms_secret.cbh_db_mirror_user_secret.plaintext).username,
    mirror-user-password : jsondecode(data.google_kms_secret.cbh_db_mirror_user_secret.plaintext).password
  }
}

// add iam
resource "google_storage_bucket_iam_member" "legacy_bucket_writer_for_cloudsql_svc_acct" {
  bucket     = var.gcs_bucket_name
  role       = "roles/storage.legacyBucketWriter"
  member     = "serviceAccount:${var.cloud_sql_instance_svc_acct}"
  depends_on = [module.db_export_to_bq]
}

resource "google_project_iam_member" "cloudsql_client_for_mirror_svc_acct" {
  project    = var.cloud_sql_instance_project_id
  role       = "roles/cloudsql.client"
  member     = "serviceAccount:${module.db_export_to_bq.mirror_svc_account_email}"
  depends_on = [module.db_export_to_bq]
}

resource "google_project_iam_member" "cloudsql_viewer_for_mirror_svc_acct" {
  project    = var.cloud_sql_instance_project_id
  role       = "roles/cloudsql.viewer"
  member     = "serviceAccount:${module.db_export_to_bq.mirror_svc_account_email}"
  depends_on = [module.db_export_to_bq]
}
