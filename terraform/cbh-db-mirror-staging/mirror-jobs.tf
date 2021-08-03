provider "kubernetes" {
  alias   = "cityblock-orchestration"
  version = "~> 1.10.0"

  // TODO: get config context cluster details from cloud composer resource (do in other places as well)
  config_context_cluster = "gke_cityblock-orchestration_us-east4-a_us-east4-prod-airflow-d4e023bb-gke" // test cluster
}

locals {
  kms_mem_svc_db_mirror_user_staging           = data.terraform_remote_state.cbh_kms_ref.outputs.mem_svc_staging_db_mirror_user
  member_index_cloudsql_svc_acct_email_staging = data.terraform_remote_state.cbh_root_ref.outputs.staging_member_index_svc_acct

  services_cloudsql_svc_acct_email_staging = data.terraform_remote_state.cbh_services_staging_ref.outputs.services_cloudsql_svc_acct_email
  kms_qm_staging_mirror_user_creds         = data.terraform_remote_state.cbh_kms_ref.outputs.qm_staging_db_mirror_user
}

// DATAFLOW TEMP BUCKET - To store temp files generated during DAG scio run.
// NOTE: Add mirror_svc_acct email to "roles/storage.admin" after applying export_to_bq modules below
module "db_mirror_dataflow_temp_bucket_staging" {
  source     = "../src/resource/storage/bucket"
  name       = "db-mirror-dataflow-temp-staging"
  project_id = module.db_mirror_staging.project_id

  // legacy roles set according to Best Practice section here: https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#cloud_dataflow_service_account
  // For more on the storage roles, see: https://cloud.google.com/storage/docs/access-control/iam-roles
  bucket_policy_data = [
    {
      role    = "roles/storage.legacyBucketOwner"
      members = ["projectOwner:${module.db_mirror_staging.project_id}", "projectEditor:${module.db_mirror_staging.project_id}"]
    },
    {
      role    = "roles/storage.legacyBucketReader"
      members = ["projectViewer:${module.db_mirror_staging.project_id}"]
    },
    { role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.qm-service-db-mirror-staging.mirror_svc_account_email}",
        "serviceAccount:${module.commons_mirror_staging.mirror_svc_account_email}",
        "serviceAccount:${module.member_index_db_mirror_staging.mirror_svc_account_email}"
      ]
    }
  ]
}

// CLOUDSQL EXPORT / MIRROR
module "member_index_db_mirror_staging" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source                        = "../src/custom/cloudsql_export_to_bq"
  project_id                    = module.db_mirror_staging.project_id
  environment                   = "staging"
  gcs_bucket_name               = "cbh-cloud-sql-export-member-index-staging"
  bq_dataset_id                 = "member_index_mirror"
  database_name                 = "member-index"
  db_login_kms_ring_name        = local.kms_mem_svc_db_mirror_user_staging.ring_name
  db_login_kms_key_name         = local.kms_mem_svc_db_mirror_user_staging.key_name
  db_login_filename_enc         = local.kms_mem_svc_db_mirror_user_staging.file_names.db_mirror_user_64_enc
  db_login_kms_self_link        = local.kms_mem_svc_db_mirror_user_staging.self_link
  cloud_sql_instance_project_id = "staging-cityblock-data"
  cloud_sql_instance_svc_acct   = local.member_index_cloudsql_svc_acct_email_staging
}

module "qm-service-db-mirror-staging" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source                        = "../src/custom/cloudsql_export_to_bq"
  project_id                    = module.db_mirror_staging.project_id
  environment                   = "staging"
  gcs_bucket_name               = "cbh-cloud-sql-export-quality-measure-staging"
  bq_dataset_id                 = "quality_measure_mirror"
  database_name                 = "quality-measure"
  db_login_kms_ring_name        = local.kms_qm_staging_mirror_user_creds.ring_name
  db_login_kms_key_name         = local.kms_qm_staging_mirror_user_creds.key_name
  db_login_filename_enc         = local.kms_qm_staging_mirror_user_creds.file_names.db_mirror_user_64_enc
  db_login_kms_self_link        = local.kms_qm_staging_mirror_user_creds.self_link
  cloud_sql_instance_project_id = "cbh-services-staging"
  cloud_sql_instance_svc_acct   = local.services_cloudsql_svc_acct_email_staging
}

// EXTERNAL DB EXPORT / MIRROR - Note different source field.
module "commons_mirror_staging" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source          = "../src/custom/db_export_to_bq"
  environment     = "staging"
  project_id      = module.db_mirror_staging.project_id
  database_name   = "commons"
  gcs_bucket_name = "cbh-export-commons-staging"
  bq_dataset_id   = "commons_mirror"
}
