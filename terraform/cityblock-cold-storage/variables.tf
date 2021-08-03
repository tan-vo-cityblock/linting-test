variable "billing_account" {
  type        = "string"
  description = "Google Cloud Billing account"
  default     = "017D3B-5DF7F9-201434"
}

variable "project_id" {
  type        = "string"
  description = "Default project id"
  default     = "cityblock-cold-storage"
}

variable "project_name" {
  type        = "string"
  description = "Default project name"
  default     = "Cityblock Cold Storage"
}

variable "default_region" {
  type        = "string"
  description = "Default region"
  default     = "us-east4"
}
