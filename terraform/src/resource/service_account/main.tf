output "email" {
  value = google_service_account.service_account.email
}

output "name" {
  value = google_service_account.service_account.name
}

output "account_id" {
  value = google_service_account.service_account.account_id
}

variable "account_id" {
  type        = string
  description = "ID for the service account"
}

variable "description" {
  type        = string
  description = "Description of use case of the service account"
  default     = ""
}

variable "project_id" {
  type        = string
  description = "Project where service account will be created"
}

resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = var.account_id
  display_name = var.account_id
  description  = var.description
}
