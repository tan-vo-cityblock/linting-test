variable "secret_id" {
  type        = string
  description = "ID for the secret manager secret"
}

variable "secret_version" {
  type        = string
  description = "ID for the secret manager secret version. Defaults to latest"
  default     = "latest"
}

variable "secret_key" {
  type        = string
  description = "Key for secret data in Kubernetes."
  default     = "latest" # to be backwards compatible
}

module "cbh_secrets_ref" {
  source     = "../../data/project"
  project_id = "cbh-secrets"
}

data "google_secret_manager_secret_version" "version" {
  project = module.cbh_secrets_ref.project_number
  secret  = var.secret_id
  version = var.secret_version
}

module "k8s_secret_service_account_creds" {
  source      = "../../resource/kubernetes"
  secret_name = var.secret_id
  secret_data = {
    "${var.secret_key}" = data.google_secret_manager_secret_version.version.secret_data
  }
}
