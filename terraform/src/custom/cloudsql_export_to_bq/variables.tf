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
  description = "Instance name for the Cloud SQL database to be mirrored. NOTE: the name needs to NOT include underlines to match GCP regex standards"
}

variable "gcs_bucket_name" {
  type        = string
  description = "Bucket containing the CSV exports from Cloud SQL"
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

variable "cloud_sql_instance_project_id" {
  type        = string
  description = "Project ID containing the Cloud SQL Instance"
}

variable "cloud_sql_instance_svc_acct" {
  type        = string
  description = "Email for service account for Cloud SQL instance"
}

variable "db_login_kms_ring_name" {
  type        = string
  description = "kms ring name required to make GCS URI to db login creds"
}

variable "db_login_kms_key_name" {
  type        = string
  description = "kms key name required to make GCS URI to db login creds"
}

variable "db_login_filename_enc" {
  type        = string
  description = "Name of encrypted file holding db login credentials in GCS"
}

variable "db_login_kms_self_link" {
  type        = string
  description = "kms self link name required to decrypt login creds to database"
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

