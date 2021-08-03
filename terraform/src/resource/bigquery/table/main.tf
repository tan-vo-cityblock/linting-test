output "table_id" {
  value = google_bigquery_table.table.table_id
}

variable "project_id" {
  type        = "string"
  description = "Project to store the table"
}

variable "dataset_id" {
  type        = "string"
  description = "Dataset to store the table"
}

variable "table_id" {
  type        = "string"
  description = "Name of the table"
}

variable "friendly_name" {
  type        = "string"
  description = "Descriptive name for the table"
  default     = ""
}

variable "description" {
  type        = "string"
  description = "Description of table"
}

variable "schema" {
  type        = "string"
  description = "Path to table schema json file"
}

variable "time_partitioning" {
  type        = object({ type = string })
  description = "The only type supported is DAY, which will generate one partition per day based on data loading time."
  default     = null
}

resource "google_bigquery_table" "table" {
  project       = var.project_id
  dataset_id    = var.dataset_id
  table_id      = var.table_id
  friendly_name = var.friendly_name
  description   = var.description
  schema        = var.schema

  dynamic "time_partitioning" {
    for_each = var.time_partitioning == null ? [] : [var.time_partitioning]
    content {
      type = time_partitioning.value.type
    }
  }
}
