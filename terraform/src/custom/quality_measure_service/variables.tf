
variable "project_id" {
  type        = "string"
  description = "Default project id where app is located, should be cbh-services-[env]"
}

variable "cloud_sql_instance" {
  type        = "string"
  description = "Instance name for Cloud SQL database the Quality Measure Service connects to"
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
