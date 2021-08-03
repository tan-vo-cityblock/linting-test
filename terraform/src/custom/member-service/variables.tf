variable "project_id" {
  type        = "string"
  description = "Default project id"
}

variable "database_project" {
  type        = "string"
  description = "Project containing Postgres Database for app"
}

variable "cloud_sql_instance" {
  type        = "string"
  description = "Instance name for Cloud SQL database the Member Service connects to"
}

variable "db_user" {
  type        = "string"
  description = "Username for PostgreSQL database"
  default     = "mixer-bot"
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

variable "project_key_ring_name" {
  type        = "string"
  description = "The name of the key ring holding the key to the encrypted app.yaml file"
}

variable "app_yaml_key_name" {
  type        = "string"
  description = "The name of the key used to decrypt app.yaml file"
}

variable "db_password_key_name" {
  type        = "string"
  description = "The name of the key used to decrypt password.txt file"
}

variable "app_engine_svc_acct_name" {
  type        = string
  description = "Fully qualified name of default App Engine service account"
}
