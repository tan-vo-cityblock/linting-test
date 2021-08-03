variable "billing" {
  description = "Google Cloud Billing account"
} # passed in from root `output.tf`

variable "sendgrid_api_key" {
  description = "API key for Sendgrid service"
} # passed in from root `terraform.tfvars`

variable "tanjin_compute_service_account" {
  description = "Service account used for example_scio DAG"
}

variable "project_id" {
  description = "Project ID to create and default to"
  default     = "cityblock-orchestration"
}

variable "from_email" {
  type    = "string"
  default = "prod@cityblock.engineering"
}

variable "test_email" {
  type    = "string"
  default = "test@cityblock.engineering"
}

variable "project_name" {
  type    = "string"
  default = "Cityblock Orchestration"
}

variable "default_region" {
  type    = "string"
  default = "us-east4"
}

variable "composer_version" {
  type    = "string"
  default = "composer-1.13.4-airflow-1.10.12"
}

variable "python_version" {
  default = 3
}
