output "omnibus_backup_bucket" {
  value       = module.gitlab_backup.name
  description = "Name of the backup bucket used for Omnibus application"
}
