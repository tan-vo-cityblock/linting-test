data "google_app_engine_default_service_account" "app_engine_svc_acct" {
  project = module.member_service_staging_project.project_id
}
