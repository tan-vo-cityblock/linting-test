variable "secret_id" {
  type        = string
  description = "ID of the Secret object"
}

variable "secret_data" {
  type        = string
  description = "The secret data. Must be no larger than 64KiB. Note: This property is sensitive and will not be displayed in the plan."
}

resource "google_secret_manager_secret_version" "secret_version" {
  provider    = google-beta
  secret      = var.secret_id
  secret_data = var.secret_data
}
