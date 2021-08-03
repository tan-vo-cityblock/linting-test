output "object" {
  value       = data.http.object.body
  description = "The contents of the object"
}

variable "object_bucket" {
  type        = "string"
  description = "Bucket name excluding 'gs://'"
}

variable "object_path" {
  type        = "string"
  description = <<EOF
    URI path of the object excluding the bucket and including the name of the object.
    REQUIRED: Content-Type of object must be "text/plain" or "application/json"
    EOF
}

data "google_client_config" "current" {}

data "google_storage_bucket_object" "object_data" {
  name   = var.object_path
  bucket = var.object_bucket
}

data "http" "object" {
  url = format("%s?alt=media", data.google_storage_bucket_object.object_data.self_link)

  // Optional request headers
  request_headers = {
    "Authorization" = "Bearer ${data.google_client_config.current.access_token}"
  }
}
