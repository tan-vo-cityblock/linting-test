output "dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}

variable "dataset_id" {
  type        = "string"
  description = "Name of dataset"
}

variable "description" {
  type        = "string"
  description = "(Optional) A user-friendly description of the dataset"
  default     = null
}
variable "project" {
  type        = "string"
  description = "Project where dataset will be created"
}

variable "special_group_access" {
  type        = list(object({ special_group = string, role = string }))
  description = "A special group to grant access to."
  default     = []
}

variable "user_access" {
  type        = list(object({ email = string, role = string }))
  description = "Users to grant access to by email."
  default     = []
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Map of labels to apply to the Dataset resource"
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id  = var.dataset_id
  project     = var.project
  description = var.description
  labels      = var.labels
  dynamic "access" {
    for_each = var.special_group_access
    content {
      special_group = access.value.special_group
      role          = access.value.role
    }
  }

  dynamic "access" {
    for_each = var.user_access
    content {
      user_by_email = access.value.email
      role          = access.value.role
    }
  }

  # TODO: configure group email access here (PLAT-1114)
}
