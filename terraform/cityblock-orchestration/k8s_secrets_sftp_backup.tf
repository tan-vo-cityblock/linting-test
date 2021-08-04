module "sftp_backup_svc_acct" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_cold_storage_project_ref.project_id
  account_id = "cbh-sftp-drop-backup"
}

resource "google_service_account_key" "sftp_backup_key" {
  service_account_id = "projects/${module.cityblock_cold_storage_project_ref.project_id}/serviceAccounts/${module.sftp_backup_svc_acct.email}"
}

module "k8s_sftp_backups_secret" {
  source      = "../src/resource/kubernetes"
  secret_name = "tf-sftp-backup"
  secret_data = {
    "key.json" : base64decode(google_service_account_key.sftp_backup_key.private_key)
  }
}
