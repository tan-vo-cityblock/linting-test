resource "google_cloud_scheduler_job" "job" {
  name             = "create-cureatr-rx-batch-list"
  description      = "Invokes cloud functions that pulls messages from Commons Appointments Pub/Sub subscription"
  project          = var.partner_project_production
  schedule         = "0 * * * *"
  time_zone        = "America/New_York"
  region           = "us-east4"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.cureatr_batch_meds_refreshes.https_trigger_url

    oidc_token {
      service_account_email = module.prod_cureatr_worker_svc_acct.email
    }
  }
}
