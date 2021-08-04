module "sftp_backup_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = var.project_id
  account_id = "cbh-sftp-drop-backup"
}
