resource "google_storage_bucket" "internal_tmp_cbh_staging" {
  name          = "internal-tmp-cbh-staging"
  project       = "${var.project_staging}"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
}

resource "google_storage_bucket_iam_member" "dataflow_temp_location_editor_data_team" {
  bucket = "${google_storage_bucket.internal_tmp_cbh_staging.name}"
  role   = "roles/storage.objectAdmin"
  member = "group:data-team@cityblock.com"
}

module "zendesk_export_staging_bucket" {
  source             = "../src/resource/storage/bucket"
  name               = "zendesk_export_staging"
  project_id         = var.project_staging
  bucket_policy_data = [
    { role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "group:eng-all@cityblock.com",
        "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}"
      ]
    }
  ]
  lifecycle_rules = [
    {
      action         = "Delete"
      age            = 2
      storage_class  = null
      created_before = null
      with_state     = null
    }
  ]
}
