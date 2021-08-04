# great_expectations
module "svc_acct_ge" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  project_id = var.project_id
  source     = "../src/custom/svc_acct_key_k8_secret"
  account_id = "great-expectations"

  account_description = <<EOF
  Service account used for running great expectations workflow in Airflow impacting cityblock-analytics resources.
  EOF
}

resource "google_project_iam_member" "svc_acct_ge_job_user" {
  member  = "serviceAccount:${module.svc_acct_ge.email}"
  role    = "roles/bigquery.jobUser"
  project = var.project_id
}

resource "google_project_iam_member" "svc_acct_ge_data_editor" {
  member  = "serviceAccount:${module.svc_acct_ge.email}"
  role    = "roles/bigquery.dataEditor"
  project = var.project_id
}
