variable "project_id" {
  type        = "string"
  description = "Default project id"
}

variable "deployment_branch_name" {
  type        = "string"
  description = "Regex that matches the code repository branch name that triggers a deployment"
  default     = null
}

variable "deployment_tag_name" {
  type        = "string"
  description = "Regex that matches the code repository tag name that triggers a deployment"
  default     = null
}

variable "cloud_sql_instance" {
  type        = "string"
  description = "Instance name for Cloud SQL database the Health Events Service connects to"
}

variable "database_name" {
  type        = "string"
  description = "The name of the database within the Cloud SQL instance"
  default     = "health_events"
}
