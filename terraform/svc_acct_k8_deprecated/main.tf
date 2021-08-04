// DEPRECATED: USE terraform/src/custom/svc_acct_key_k8_secret/main.tf INSTEAD
output "service_account_email" {
  value = google_service_account.service_account.email
}

output "svc_acct_name" {
  value = google_service_account.service_account.name
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

resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = "tf-svc-${var.account_id}"
  display_name = "tf-svc-${var.account_id}"
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = google_service_account.service_account.name
}

resource "kubernetes_secret" "google-application-credentials" {
  metadata {
    name = "tf-svc-${var.account_id}"
  }

  data = {
    "key.json" = base64decode(google_service_account_key.service_account_key.private_key)
  }
}
