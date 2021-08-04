variable "app_engine_project_id" {
  type        = string
  description = "Project hosting app engine app"
}

variable "cloudbuild_project_id" {
  type        = string
  description = "Project hosting cloudbuild that will build / deploy the app engine app"
  default     = "cityblock-data"
}

variable "grant_cloudsql_perms" {
  type        = bool
  description = <<EOT
  true, if the cloudbuild service account should be granted perms to access the
  app's cloudsql resource. Otherwise false, if the app doesn't use cloudsql"
  EOT
}

variable "cloudsql_project_id" {
  type        = string
  description = "Project hosting cloudsql resource if different from app engine project"
  default     = null
}

module "app_engine_project_ref" {
  source     = "../../data/project"
  project_id = var.app_engine_project_id
}

module "cloudbuild_project_ref" {
  source     = "../../data/project"
  project_id = var.cloudbuild_project_id
}

resource "google_project_iam_member" "cloudbuild_appengine_admin" {
  project = module.app_engine_project_ref.project_id
  member  = "serviceAccount:${module.cloudbuild_project_ref.default_cloudbuild_service_account_email}"
  role    = "roles/appengine.appAdmin"
}

resource "google_project_iam_member" "cloudbuild_storage_admin" {
  project = module.app_engine_project_ref.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${module.cloudbuild_project_ref.default_cloudbuild_service_account_email}"
}

resource "google_project_iam_member" "cloudbuild_builds_editor" {
  project = module.app_engine_project_ref.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${module.cloudbuild_project_ref.default_cloudbuild_service_account_email}"
}

resource "google_service_account_iam_member" "cloudbuild_app_engine_service_acct_user" {
  member             = "serviceAccount:${module.cloudbuild_project_ref.default_cloudbuild_service_account_email}"
  role               = "roles/iam.serviceAccountUser"
  service_account_id = module.app_engine_project_ref.default_app_engine_service_account_name
}

resource "google_project_iam_member" "cloudbuild_cloudsql_client" {
  count   = var.grant_cloudsql_perms == false ? 0 : 1
  project = var.cloudsql_project_id == null ? module.app_engine_project_ref.project_id : var.cloudsql_project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${module.cloudbuild_project_ref.default_cloudbuild_service_account_email}"
}
