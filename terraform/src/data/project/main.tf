output "project_id" {
  value = data.google_project.project.project_id
}

output "project_number" {
  value       = data.google_project.project.number
  description = "The numeric identifier of the project. Computed attribute that can't be set"
}

output "default_cloudbuild_service_account_email" {
  value = "${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

output "default_compute_engine_service_account_email" {
  value = "${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

output "default_dataflow_job_runner_email" {
  value = data.google_service_account.default_dataflow_job_runner_svc_acct.email
}

output "default_dataflow_job_runner_id" {
  value = data.google_service_account.default_dataflow_job_runner_svc_acct.name
}

output "default_cloudservices_service_account_email" {
  value = "${data.google_project.project.number}@cloudservices.gserviceaccount.com"
}

output "default_gke_service_account_email" {
  value = "service-${data.google_project.project.number}@container-engine-robot.iam.gserviceaccount.com"
}

output "default_us_gcr_bucket" {
  value = "us.artifacts.${data.google_project.project.project_id}.appspot.com"
}

output "default_gcr_bucket" {
  value = "artifacts.${data.google_project.project.project_id}.appspot.com"
}

output "default_pubsub_service_account_email" {
  value = "service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

output "default_app_engine_service_account_email" {
  value       = data.google_app_engine_default_service_account.app_engine_svc_acct.email
  description = "Default app engine service account email"
}

output "default_app_engine_service_account_name" {
  value       = data.google_app_engine_default_service_account.app_engine_svc_acct.name
  description = "Default app engine service account name"
}

variable "project_id" {
  type = "string"
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_app_engine_default_service_account" "app_engine_svc_acct" {
  project = data.google_project.project.project_id
}

data "google_service_account" "default_dataflow_job_runner_svc_acct" {
  account_id = "dataflow-job-runner"
  project = data.google_project.project.project_id
}
