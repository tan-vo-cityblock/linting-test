// TODO doesn't seem like an ideal place to contain a staging image (PLAT-992)
resource "google_project_iam_member" "image_access" {
  project = "cityblock-data"
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${module.member_service_staging_project.project_number}-compute@developer.gserviceaccount.com"
}
