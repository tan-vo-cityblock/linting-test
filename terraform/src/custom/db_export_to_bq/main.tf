locals {
  default_bucket_policy_data = [
    { role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "group:eng-all@cityblock.com",
        "serviceAccount:${module.mirror_svc_acct_k8.email}"
      ]
    }
  ]
}

module "mirror_svc_acct_k8" {
  source              = "../svc_acct_key_k8_secret"
  project_id          = var.project_id
  account_id          = "${var.environment}-${var.database_name}-mirror"
  account_description = "Service account for mirroring ${var.database_name} into BQ"
}

module "bq_dataset" {
  source     = "../../resource/bigquery/dataset"
  project    = var.project_id
  dataset_id = var.bq_dataset_id
  labels     = var.labels
  user_access = concat([{
    email = module.mirror_svc_acct_k8.email,
    role  = "OWNER"
    }], [
    for email in var.bq_dataset_readers :
    { email = email, role = "READER" }
    ]
  )
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
}

module "gcs_cloud_sql_export_bucket" {
  source             = "../../resource/storage/bucket"
  name               = var.gcs_bucket_name
  project_id         = var.project_id
  bucket_policy_data = concat(var.bucket_policy_data, local.default_bucket_policy_data) // TODO: Refactor so internal `members` list is appended to if duplicate `roles` concatinated.
  labels             = var.labels
  lifecycle_rules = [
    {
      action         = "Delete"
      age            = var.gcs_bucket_contents_time_to_live // units in days
      storage_class  = null
      created_before = null
      with_state     = null
    }
  ]
}

resource "google_project_iam_member" "dataflow_admin" {
  project    = var.project_id
  role       = "roles/dataflow.admin"
  member     = "serviceAccount:${module.mirror_svc_acct_k8.email}"
  depends_on = [module.mirror_svc_acct_k8.email]
}

resource "google_project_iam_member" "job_user" {
  project    = var.project_id
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${module.mirror_svc_acct_k8.email}"
  depends_on = [module.mirror_svc_acct_k8.email]
}

resource "google_project_iam_member" "service_acct_actor" {  // necessary to run Dataflow jobs - see https://cloud.google.com/iam/docs/service-accounts-actas#dataproc-dataflow-datafusion
  project    = var.project_id
  role       = "roles/iam.serviceAccountUser"
  member     = "serviceAccount:${module.mirror_svc_acct_k8.email}"
}
