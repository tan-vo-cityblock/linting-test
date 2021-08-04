output "email" {
  value = module.service_account.email
}

output "svc_acct_name" {
  value = module.service_account.name
}

variable "account_id" {
  type        = string
  description = "ID for the service account"
}

variable "account_description" {
  type        = string
  description = "Description of use case of the service account"
}

variable "project_id" {
  type        = string
  description = "Which project to create the service account"
}

variable "secret_id" {
  type        = string
  description = "ID for the secret associated with the version"
}

module "service_account" {
  source     = "../../resource/service_account"
  account_id = var.account_id
  project_id = var.project_id
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = module.service_account.name
}

#TODO: FIX RESOURCE NAME "dbt-svc-cred-file" SO IT IS GENERIC SINCE IT IS IN A MODULE
resource "google_secret_manager_secret_version" "dbt-svc-cred-file" {
  provider    = google-beta
  secret      = var.secret_id
  secret_data = base64decode(google_service_account_key.service_account_key.private_key)
}
