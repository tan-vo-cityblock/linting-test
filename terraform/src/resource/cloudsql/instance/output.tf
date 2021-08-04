output "connection_name" {
  value       = google_sql_database_instance.instance.connection_name
  description = " The connection name of the instance to be used in connection strings. For example, when connecting with Cloud SQL Proxy."
}

output "svc_account_email" {
  value       = google_sql_database_instance.instance.service_account_email_address
  description = "Service account associated with Cloud SQL Instance"
}

output "name" {
  value       = google_sql_database_instance.instance.name
  description = "Name of the instance"
}
