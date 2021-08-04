output "name" {
  value = google_storage_bucket.bucket.name
}

variable "name" {
  type        = "string"
  description = "Name of bucket"
}

variable "project_id" {
  type        = "string"
  description = "Project where bucket will be created"
}

variable "location" {
  type        = "string"
  description = "GCS Location"
  default     = "US"
}

variable "storage_class" {
  type        = "string"
  description = "The Storage Class of the new bucket. Supported values include: STANDARD, MULTI_REGIONAL, REGIONAL, NEARLINE, COLDLINE."
  default     = "MULTI_REGIONAL"
}

variable "encryption" {
  type        = object({ default_kms_key_name = string })
  description = "The bucket's encryption configuration. The block supports A Cloud KMS key that will be used to encrypt objects inserted into this bucket."
  default     = null
}

variable "bucket_policy_data" {
  type        = list(object({ role = string, members = list(string) }))
  description = <<EOF
    Authoritative for a given role. Updates the IAM policy to grant a role to a list of members.
    Other roles within the IAM policy for the storage bucket are preserved.
    See for more details on roles: https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles
  EOF
}

variable "bucket_policy_only" {
  type        = bool
  default     = false
  description = "(Optional, Default: false) Enables Bucket Policy Only access to a bucket. See more here: https://cloud.google.com/storage/docs/uniform-bucket-level-access"
}
// see documentation on bucket lifecycle management: https://cloud.google.com/storage/docs/lifecycle
variable "lifecycle_rules" {
  type        = list(object({ action = string, storage_class = string, age = number, created_before = string, with_state = string })) // add more if need be
  description = "lifecycle configuration for rules to apply to bucket if need be"
  default     = []
}

variable "cors" {
  type        = list(object({ origin = list(string), method = list(string), response_header = list(string), max_age_seconds = number }))
  description = "The bucket's Cross-Origin Resource Sharing (CORS) configuration. See for details: https://www.terraform.io/docs/providers/google/r/storage_bucket.html#cors"
  default     = []
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Map of labels to apply to the Storage bucket resource"
}

variable "versioning" {
  type        = bool
  default     = true
  description = "retains a noncurrent object version when the live object version gets replaced or deleted"
}

resource "google_storage_bucket" "bucket" {
  name                        = var.name
  location                    = var.location
  project                     = var.project_id
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.bucket_policy_only
  labels                      = var.labels

  versioning {
    enabled = var.versioning
  }

  dynamic "encryption" {
    for_each = var.encryption == null ? [] : [var.encryption]
    content {
      default_kms_key_name = encryption.value.default_kms_key_name
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action
        storage_class = lifecycle_rule.value.storage_class
      }
      condition {
        age            = lifecycle_rule.value.age
        created_before = lifecycle_rule.value.created_before
        with_state     = lifecycle_rule.value.with_state
      }
    }
  }

  dynamic "cors" {
    for_each = var.cors
    content {
      origin          = cors.value.origin
      method          = cors.value.method
      response_header = cors.value.response_header
      max_age_seconds = cors.value.max_age_seconds
    }
  }
}

data "google_iam_policy" "bucket_policy_data" {

  dynamic "binding" { // False alarm on IDE error. Data sources do in fact support dynamic blocks.
    for_each = var.bucket_policy_data
    content {
      role    = binding.value.role
      members = binding.value.members
    }
  }
}

resource "google_storage_bucket_iam_policy" "bucket_policy" {
  bucket      = google_storage_bucket.bucket.name
  policy_data = data.google_iam_policy.bucket_policy_data.policy_data
}
