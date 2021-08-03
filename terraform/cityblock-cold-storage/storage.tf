resource "google_storage_bucket" "mattermost_backup_bucket" {
  project       = google_project.cold_storage.project_id
  name          = "mattermost-backup"
  location      = var.default_region
  storage_class = "COLDLINE"

  retention_policy {
    is_locked        = false
    retention_period = 315776000
  }
}

module "sftp_drop_backups" {
  source        = "../src/resource/storage/bucket"
  project_id    = var.project_id
  name          = "cbh-sftp-drop-backup"
  location      = var.default_region
  storage_class = "COLDLINE"
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    {
      role = "roles/storage.admin"
      members = [
        "group:platform-team@cityblock.com",
        "group:gcp-admins@cityblock.com",
        "serviceAccount:${module.sftp_backup_svc_acct.email}",
      ]
    }
  ]
}
