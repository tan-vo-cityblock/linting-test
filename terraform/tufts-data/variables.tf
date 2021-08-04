variable "partner_project_production" {
  type        = string
  description = "Tufts's production data project"
  default     = "tufts-data"
}

variable "able_health_service_account" {
  type        = string
  description = "Service account for Able Health workflows"
}

variable "orchestration_service_account" {
  description = "Service account that non-k8s DAG operators utilize by default in Cloud Composer"
}
