output "email" {
  value = module.service_account.email
}

output "svc_acct_name" {
  value = module.service_account.name
}

variable "account_id" {
  type        = "string"
  description = "ID for the service account"
}

variable "account_description" {
  type        = "string"
  description = "Description of use case of the service account"
}

variable "project_id" {
  type        = "string"
  description = "Which project to create the service account"
}

module "service_account" {
  source     = "../../resource/service_account"
  account_id = var.account_id
  project_id = var.project_id
  description = var.account_description
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = module.service_account.name
}

module "k8s_secret_service_account_creds" {
  source      = "../../resource/kubernetes"
  secret_name = "tf-svc-${module.service_account.account_id}"
  secret_data = {
    "key.json" = base64decode(google_service_account_key.service_account_key.private_key)
  }
}
