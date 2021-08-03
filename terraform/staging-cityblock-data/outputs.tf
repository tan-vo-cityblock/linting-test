output "member_index_svc_acct" {
  value       = module.member_index_postgres_instance_staging.svc_account_email
  description = "Service account associated with Cloud SQL Instance for Member Index Staging"
}
