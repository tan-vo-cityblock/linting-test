module "sftp_drop_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = var.partner_project_production
  name          = "cbh_sftp_drop"
  storage_class = "MULTI_REGIONAL"
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    {
      role = "roles/storage.objectViewer",
      members = [
        "group:integrations@cityblock.com",
        "group:actuary@cityblock.com",
        "group:data-team@cityblock.com",
        "group:sftp-bucket-access@cityblock.com",
        "serviceAccount:${module.sftp_backup_svc_acct.email}",
        "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}",
        "serviceAccount:${module.cityblock_orchestration_project_ref.default_compute_engine_service_account_email}",
        "serviceAccount:${var.load_emblem_monthly_svc_acct}",
        "serviceAccount:${var.load_emblem_weekly_svc_acct}",
        "serviceAccount:${var.load_cci_monthly_svc_acct}",
        "serviceAccount:${module.development_dataflow_svc_acct.email}",
        "serviceAccount:${module.svc_acct_cardinal_worker.email}",
        "serviceAccount:${module.svc_acct_healthyblue_worker.email}",
        "serviceAccount:${var.load_gcs_data_cf_svc_acct}"
      ]
    },
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com",
        "user:katie.claiborne@cityblock.com",
        "serviceAccount:${module.load_tufts_daily_service_account.email}",
        "serviceAccount:${module.svc_acct_carefirst_worker.email}"
      ]
    },
    {
      role = "roles/storage.objectAdmin"
      members = [
        "serviceAccount:${var.load_gcs_data_cf_svc_acct}",
        "serviceAccount:${module.sftp_sync_svc_acct.email}",
        "serviceAccount:${var.payer_suspect_service_account_email}"
      ]
    }
  ]
}

module "us_containers_bucket" {
  source        = "../src/resource/storage/bucket"
  project_id    = var.partner_project_production
  name          = "us.artifacts.cityblock-data.appspot.com"
  storage_class = "STANDARD"
  versioning    = false
  bucket_policy_data = [
    {
      role = "roles/storage.objectViewer",
      members = [
        "group:data-team@cityblock.com"
      ]
    },
    {
      role = "roles/storage.admin"
      members = [
        "group:eng-all@cityblock.com"
      ]
    }
  ]
}

resource "google_storage_bucket" "container-images" {
  name          = "artifacts.cityblock-data.appspot.com"
  storage_class = "STANDARD"
}

resource "google_storage_bucket_iam_member" "orchestration-cloudbuild-access" {
  bucket = google_storage_bucket.container-images.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.cityblock_orchestration_project_ref.default_cloudbuild_service_account_email}"
}

resource "google_storage_bucket" "caresource" {
  name    = "cbh_caresource_interchange"
  project = var.partner_project_production
  labels = {
    data = "phi"
  }
  versioning {
    enabled = true
  }
}

data "google_iam_policy" "caresource_policy" {
  binding {
    role    = "roles/storage.admin"
    members = ["group:gcp-admins@cityblock.com"]
  }

  binding {
    role = "roles/storage.objectAdmin"
    members = [
      "group:gcp-admins@cityblock.com",
      "user:mary.adomshick@oliverwyman.com",
      "user:friederike.schuur@cityblock.com",
      "group:actuary@cityblock.com"
    ]
  }
}

resource "google_storage_bucket_iam_policy" "caresource_bucket_policy" {
  bucket      = google_storage_bucket.caresource.name
  policy_data = data.google_iam_policy.caresource_policy.policy_data
}

module "partner_configs_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-partner-configs"
  project_id = var.partner_project_production
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.load_tufts_daily_service_account.email}",
        "serviceAccount:${var.payer_suspect_service_account_email}"
      ]
    }
  ]
}

module "raw_transforms_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-raw-transforms"
  project_id = var.partner_project_production
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/storage.objectAdmin"
      members = [
        "group:gcp-admins@cityblock.com"
      ]
    }
  ]
}

module "cloud_build_logs_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "cbh-cloud-build-logs"
  project_id = var.partner_project_production
  bucket_policy_data = [
    {
      role = "roles/storage.objectViewer"
      members = [
        "group:data-team@cityblock.com"
      ]
    },
    {
      role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.cityblock_data_project_ref.default_cloudbuild_service_account_email}"
      ]
    }
  ]
}

// DATAFLOW TEMP BUCKET - To store temp files generated during AIRFLOW DAG scio runs.
// Add the service account used in Airflow to `roles/storage.admin`
module "cityblock_data_dataflow_temp_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${var.partner_project_production}-dataflow-temp"
  project_id = var.partner_project_production

  // legacy roles set according to Best Practice section here: https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#cloud_dataflow_service_account
  // For more on the storage roles, see: https://cloud.google.com/storage/docs/access-control/iam-roles
  bucket_policy_data = [
    {
      role    = "roles/storage.legacyBucketOwner"
      members = ["projectOwner:${var.partner_project_production}", "projectEditor:${var.partner_project_production}"]
    },
    {
      role    = "roles/storage.legacyBucketReader"
      members = ["projectViewer:${var.partner_project_production}"]
    },
    { role = "roles/storage.admin"
      members = [ //add mirror service account here after creating cloudsql_export_to_bq modules
        "group:gcp-admins@cityblock.com",
        "group:data-team@cityblock.com",
        "serviceAccount:${module.elation_worker_svc_acct.email}",
        "serviceAccount:${module.redox_worker_svc_acct.email}",
        "serviceAccount:${module.svc_acct_carefirst_worker.email}"
      ]
    }
  ]
}

module "zendesk_export_prod_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "zendesk_export_prod"
  project_id = var.partner_project_production
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    { role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "group:eng-all@cityblock.com",
        "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}"
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

module "temp_cureatr_rx_batch_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "temp-cureatr-rx-refresh"
  project_id = var.partner_project_production
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}",
      ]
    }
  ]
}

