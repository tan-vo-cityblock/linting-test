variable "partner_project_production" {
  type        = string
  description = "Connecticare's production data project"
  default     = "connecticare-data"
}

variable "orchestration_service_account" {
  description = "Service account that non-k8s DAG operators utilize by default in Cloud Composer"
}

variable "payer_suspect_service_account_email" {
  description = "Service account for payer suspect coding"
}

variable "able_health_service_account" {
  type        = string
  description = "Service account for Able Health workflows"
}
