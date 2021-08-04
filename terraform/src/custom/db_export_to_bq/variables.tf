variable "environment" {
  type        = string
  description = "Environment we are provisioning for"
}

variable "project_id" {
  type        = string
  description = "Default project name"
}

variable "database_name" {
  type        = string
  description = "Instance name for the database to be mirrored."
}

variable "gcs_bucket_name" {
  type        = string
  description = "GCS Bucket to hold the database CSV exports"
}

variable "bucket_policy_data" {
  type        = list(object({ role = string, members = list(string) }))
  description = <<EOF
    Authoritative for a given role. Updates the IAM policy to grant a role to a list of members.
    Other roles within the IAM policy for the storage bucket are preserved.
    See for more details on roles: https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles
  EOF
  default     = []
}

variable "gcs_bucket_contents_time_to_live" {
  type        = number
  description = "Length of time objects exist in bucket before getting deleted"
  default     = 2
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset name for mirroring output"
}

variable "bq_dataset_readers" {
  type        = list(string)
  description = "Emails (User or Service Account) with read access to dataset"
  default     = []
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Map of labels to apply to the Storage bucket and Dataset resources"
}
