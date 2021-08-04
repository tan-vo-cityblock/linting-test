output "services_cloudsql_svc_acct_email" {
  value       = module.services_postgres_instance.svc_account_email
  description = "Service account associated with Cloud SQL Instance for Services Staging"
}
