module "cityblock_production_patient_data_bucket" {
  source        = "../src/resource/storage/bucket"
  name          = "cityblock-production-patient-data"
  project_id    = google_project.commons-production.project_id
  storage_class = "STANDARD"
  labels = {
    data = "phi"
  }
  cors = [
    {
      origin          = ["https://commons.cityblock.com"],
      method          = ["GET", "PUT", "POST", "DELETE"],
      response_header = ["*"],
      max_age_seconds = 3600
    }
  ]
  // legacy roles set according to Best Practice section here: https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#cloud_dataflow_service_account
  // For more on the storage roles, see: https://cloud.google.com/storage/docs/access-control/iam-roles
  bucket_policy_data = [
    {
      role = "roles/storage.legacyBucketOwner"
      members = [
        "projectOwner:${google_project.commons-production.project_id}",
      "projectEditor:${google_project.commons-production.project_id}"]
    },
    {
      role = "roles/storage.legacyBucketReader"
      members = [
        "projectViewer:${google_project.commons-production.project_id}",
        "user:fernando@cityblock.com",
        "user:marcy@cityblock.com",
        "user:theresa@cityblock.com"
      ]
    },
    {
      role = "roles/storage.legacyObjectReader"
      members = [
        "user:fernando@cityblock.com",
        "user:marcy@cityblock.com",
        "user:theresa@cityblock.com"
      ]
    },
    { role = "roles/storage.admin"
      members = [
        "group:gcp-admins@cityblock.com",
      ]
    },
    { role = "roles/storage.objectAdmin"
      members = [
        "serviceAccount:${module.elation_worker_svc_acct.email}",
        "serviceAccount:${module.cityblock_data_project_ref.default_compute_engine_service_account_email}",
        "serviceAccount:${google_service_account.commons-production.email}",
        "serviceAccount:${module.dataflow_job_runner_svc_acct.email}",
        "serviceAccount:${module.patient_eligibilities_svc_acct.email}",
        "serviceAccount:${module.panel_management_svc_acct.email}",
        "serviceAccount:${module.redox_worker_svc_acct.email}",
        "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}"
      ]
    }
  ]
}
