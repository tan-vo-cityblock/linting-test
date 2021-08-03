resource "google_project_iam_member" "data_team_viewers" {
  project = module.git_project.project_id
  member  = "group:data-team@cityblock.com"
  role    = "roles/viewer"
}

resource "google_storage_bucket_iam_member" "gitlab_pusher_admin_cbh_git_us_gcr" {
  bucket = module.git_project.default_us_gcr_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.cityblock_orchestration_project_ref.default_compute_engine_service_account_email}"
}
