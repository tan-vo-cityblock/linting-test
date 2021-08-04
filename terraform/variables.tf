variable "billing_account" {
  description = "Google Cloud Billing account"
  default     = "017D3B-5DF7F9-201434"
}

variable "primary_cluster" {
  description = "Name of primary Kubernetes cluster in Kubernetes Engine"
  default     = "gke_cityblock-data_us-east1-b_airflow"
}

variable "sendgrid_api_key" {}
variable "pagerduty_api_key" {}
variable "platform_bot_slack_secret" {}
# passed in via terraform.tfvars
