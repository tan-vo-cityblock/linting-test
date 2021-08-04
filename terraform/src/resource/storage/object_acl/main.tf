variable "object_bucket" {
  type        = "string"
  description = "Bucket name excluding 'gs://'"
}

variable "object_path" {
  type        = "string"
  description = "URI path of the object excluding the bucket and including the name of the object"
}

variable "role_entities" {
  type        = list(string)
  description = <<EOF
    List of role/entity pairs in the form ROLE:entity.
    See GCS Object ACL documentation for more details - https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls.
    Must be set if predefined_acl is not.
    EOF
}

resource "google_storage_object_acl" "object_acl" {
  bucket      = var.object_bucket
  object      = var.object_path
  role_entity = var.role_entities
}
