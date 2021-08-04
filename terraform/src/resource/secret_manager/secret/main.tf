locals {
  project_id = "cbh-secrets"  # all secrets should be managed in this project
}

output "id" {
  value       = google_secret_manager_secret.secret.id
  description = "ID of the Secret object"
}

output "secret_id" {
  value       = google_secret_manager_secret.secret.secret_id
  description = "secret_id of the Secret object, this matches the value passed in and is distinct from secret.id"
}

variable "secret_id" {
  type        = string
  description = "Name of the Secret"
}

variable "replication_locations" {
  type        = list(string)
  description = "Locations to contain"
  default     = ["us-east4"]
}

variable "secret_accessors" {
  type        = list(string)
  description = "Entities that have access to the secret, must be in the form: type:email@cityblock.com"
  default     = []
}

resource "google_secret_manager_secret" "secret" {
  provider  = google-beta
  project   = local.project_id
  secret_id = var.secret_id
  replication {
    # only allowing user managed replications for secrets (must be US based location)
    user_managed {
      dynamic "replicas" {
        for_each = var.replication_locations
        content {
          location = replicas.value
        }
      }
    }
  }
}

resource "google_secret_manager_secret_iam_binding" "admin_binding" {
  provider  = google-beta
  project   = local.project_id
  secret_id = google_secret_manager_secret.secret.secret_id
  role      = "roles/secretmanager.admin"
  members   = ["group:gcp-admins@cityblock.com"]
}

resource "google_secret_manager_secret_iam_binding" "access_binding" {
  provider  = google-beta
  project   = local.project_id
  secret_id = google_secret_manager_secret.secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  members   = concat(["group:eng-all@cityblock.com"], var.secret_accessors)
}
