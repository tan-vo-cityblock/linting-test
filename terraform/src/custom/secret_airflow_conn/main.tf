variable "service_account_name" {
  type = "string"
  description = "name of service account we would like to create a key for"
}

variable "service_account_id" {
  type = "string"
  description = "id of service account we would like to create a key for"
}

variable "storage_bucket" {
  type = string
  description = "GCS bucket to store the secret in"
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = var.service_account_name
}

resource "google_storage_bucket_object" "service_account_json" {
  name    = "data/tf-svc-${var.service_account_id}.json"
  bucket  = var.storage_bucket
  content = base64decode(google_service_account_key.service_account_key.private_key)
}
