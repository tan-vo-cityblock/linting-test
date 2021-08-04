variable "view_sql" {
  type        = "string"
  description = "SQL for View"
}

variable "project_id" {
  type        = "string"
  description = "Project to store the view"
}

variable "dataset_id" {
  type        = "string"
  description = "Dataset to store the view"
}

variable "view_name" {
  type        = "string"
  description = "Dataset to store the view"
}

variable "description" {
  type        = "string"
  description = "Description of view"
  default     = ""
}

resource "google_bigquery_table" "view" {
  dataset_id = "${var.dataset_id}"
  table_id   = "${var.view_name}"

  project     = "${var.project_id}"
  description = "${var.description}"

  view {
    query          = "${var.view_sql}"
    use_legacy_sql = false
  }
}
