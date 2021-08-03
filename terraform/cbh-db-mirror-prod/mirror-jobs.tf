provider "kubernetes" {
  alias   = "cityblock-orchestration"
  version = "~> 1.10.0"

  // TODO: get config context cluster details from cloud composer resource (do in other places as well)
  config_context_cluster = "gke_cityblock-orchestration_us-east4-a_us-east4-prod-airflow-d4e023bb-gke" // prod cluster
}

locals {
  member_index_cloudsql_svc_acct_email_prod = data.terraform_remote_state.cbh_root_ref.outputs.prod_member_index_svc_acct
  kms_mem_svc_db_mirror_user_prod           = data.terraform_remote_state.cbh_kms_ref.outputs.mem_svc_prod_db_mirror_user

  services_cloudsql_svc_acct_email_prod = data.terraform_remote_state.cbh_services_prod_ref.outputs.services_cloudsql_svc_acct_email
  kms_qm_prod_mirror_user_creds         = data.terraform_remote_state.cbh_kms_ref.outputs.qm_prod_db_mirror_user
}

// DATAFLOW TEMP BUCKET - To store temp files generated during DAG scio run.
// NOTE: Add mirror_svc_acct email to "roles/storage.admin" after applying export_to_bq modules below
module "db_mirror_dataflow_temp_bucket_prod" {
  source     = "../src/resource/storage/bucket"
  name       = "db-mirror-dataflow-temp-prod"
  project_id = module.db_mirror_prod.project_id
  lifecycle_rules = [
    {
      action         = "Delete"
      age            = 2
      storage_class  = null
      created_before = null
      with_state     = null
    }
  ]

  // legacy roles set according to Best Practice section here: https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#cloud_dataflow_service_account
  // For more on the storage roles, see: https://cloud.google.com/storage/docs/access-control/iam-roles
  bucket_policy_data = [
    {
      role    = "roles/storage.legacyBucketOwner"
      members = ["projectOwner:${module.db_mirror_prod.project_id}", "projectEditor:${module.db_mirror_prod.project_id}"]
    },
    {
      role    = "roles/storage.legacyBucketReader"
      members = ["projectViewer:${module.db_mirror_prod.project_id}"]
    },
    { role = "roles/storage.admin"
      members = [ //add mirror service account here after creating cloudsql_export_to_bq modules
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.commons_mirror_prod.mirror_svc_account_email}",
        "serviceAccount:${module.qm_service_db_mirror_prod.mirror_svc_account_email}",
        "serviceAccount:${module.member_index_db_mirror_prod.mirror_svc_account_email}"
      ]
    }
  ]
}

// CLOUDSQL EXPORT / MIRROR
module "member_index_db_mirror_prod" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source          = "../src/custom/cloudsql_export_to_bq"
  project_id      = module.db_mirror_prod.project_id
  environment     = "prod"
  gcs_bucket_name = "cbh-cloud-sql-export-member-index-prod"
  bq_dataset_id   = "member_index_mirror"
  labels = {
    data = "phi"
  }
  bq_dataset_readers = [
    module.able_health_svc_acct.email,
    module.elation_worker_svc_acct.email,
    module.dbt_staging_svc_acct.email,
    module.dbt_prod_svc_acct.email
  ]

  database_name                 = "member-index"
  db_login_kms_ring_name        = local.kms_mem_svc_db_mirror_user_prod.ring_name
  db_login_kms_key_name         = local.kms_mem_svc_db_mirror_user_prod.key_name
  db_login_filename_enc         = local.kms_mem_svc_db_mirror_user_prod.file_names.db_mirror_user_64_enc
  db_login_kms_self_link        = local.kms_mem_svc_db_mirror_user_prod.self_link
  cloud_sql_instance_project_id = "cityblock-data"
  cloud_sql_instance_svc_acct   = local.member_index_cloudsql_svc_acct_email_prod
}

module "qm_service_db_mirror_prod" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source          = "../src/custom/cloudsql_export_to_bq"
  project_id      = module.db_mirror_prod.project_id
  environment     = "prod"
  gcs_bucket_name = "cbh-cloud-sql-export-quality-measure-prod"
  bq_dataset_id   = "quality_measure_mirror"
  labels = {
    data = "phi"
  }
  bq_dataset_readers            = [module.looker_demo.email]
  database_name                 = "quality-measure"
  db_login_kms_ring_name        = local.kms_qm_prod_mirror_user_creds.ring_name
  db_login_kms_key_name         = local.kms_qm_prod_mirror_user_creds.key_name
  db_login_filename_enc         = local.kms_qm_prod_mirror_user_creds.file_names.db_mirror_user_64_enc
  db_login_kms_self_link        = local.kms_qm_prod_mirror_user_creds.self_link
  cloud_sql_instance_project_id = "cbh-services-prod"
  cloud_sql_instance_svc_acct   = local.services_cloudsql_svc_acct_email_prod
}

// EXTERNAL DB EXPORT / MIRROR - Note different source field.
module "commons_mirror_prod" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source          = "../src/custom/db_export_to_bq"
  environment     = "prod"
  project_id      = module.db_mirror_prod.project_id
  database_name   = "commons"
  gcs_bucket_name = "cbh-export-commons-prod"
  labels = {
    data = "phi"
  }
  bq_dataset_id = "commons_mirror"
  bq_dataset_readers = [
    module.able_health_svc_acct.email,
    module.elation_worker_svc_acct.email,
    module.looker_demo.email,
    module.dbt_staging_svc_acct.email,
    module.dbt_prod_svc_acct.email,
    module.dataflow_runner_service_account.email,
    module.sli_metrics_service_account.email,
    module.ml_labeling_svc_acct.email
  ]
}

module "elation_mirror_prod" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration"
  }
  source          = "../src/custom/db_export_to_bq"
  environment     = "prod"
  project_id      = module.db_mirror_prod.project_id
  database_name   = "elation"
  gcs_bucket_name = "cbh-export-elation-prod"
  labels = {
    data = "phi"
  }
  bq_dataset_id = "elation_mirror"
  bq_dataset_readers = [
    module.looker_demo.email,
    module.elation_worker_svc_acct.email,
    module.dbt_staging_svc_acct.email,
    module.dbt_prod_svc_acct.email
  ]
}
