output "email" { // NOTE: For now, this output only applies to user defined service accounts NOT for Google managed service accounts.
  value       = data.google_service_account.service_account.email
  description = "The e-mail address of the service account. This value should be referenced from any google_iam_policy data sources that would grant the service account privileges."
}

output "name" {
  value       = data.google_service_account.service_account.name
  description = "The fully-qualified name of the service account"
}

variable "project_id" {
  type        = "string"
  description = "The ID of the project that the service account is present in. Defaults to the provider project configuration."
}

variable "account_id" {
  type        = "string"
  description = "The Service account id. This is the part of the service account's email field that comes before the @ symbol."
}

data "google_service_account" "service_account" {
  project    = var.project_id
  account_id = var.account_id
}
