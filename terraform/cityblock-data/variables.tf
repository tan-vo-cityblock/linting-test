variable "pagerduty_api_key" {
  description = "API key for Pagerduty service"
} # passed in from root `terraform.tfvars`

variable "platform_bot_slack_secret" {
  description = "Slack secret for Platform Service bot"
} # passed in from root `terraform.tfvars`

variable "partner_project_production" {
  type        = string
  description = "Monolithic data project"
  default     = "cityblock-data"
}

variable "emblem_claims_dataset" {
  type        = string
  description = "Dataset ID of Emblem claims for legacy pointer views"
}

variable "legacy_emblem_data_project" {
  type        = string
  description = "Project containing new emblem data"
}

variable "commons_prod_account" {
  type        = string
  description = "Service account for Commons related workflows"
}

variable "commons_staging_account" {
  type        = string
  description = "Email for Commons Staging service account"
}

variable "orchestration_service_account" {
  description = "Service account that non-k8s DAG operators utilize by default in Cloud Composer"
}

variable "able_health_service_account" {
  description = "Service account for Able Health workflows"
}

variable "qreviews_service_account" {
  description = "Service account for Q-Reviews workflows"
}

variable "patient_eligibilities_service_account" {
  description = "Service account for updating patient eligibilities"
}

variable "mixer_repo" {
  type        = string
  description = "Name of the Mixer repository as referenced in Source Repositories service"
  default     = "github_cityblock_mixer"
}

variable "payer_suspect_service_account_email" {
  type        = string
  description = "Service account email for payer suspect coding"
}

variable "load_emblem_monthly_svc_acct" {
  type        = string
  description = "Service account email for loading monthly data from Emblem"
}

variable "load_emblem_weekly_svc_acct" {
  type        = string
  description = "Service account email for loading weekly data from Emblem"
}

variable "load_cci_monthly_svc_acct" {
  type        = string
  description = "Service account email for loading monthly data from CCI"
}

variable "airflow_ae_cf_invoker_svc_acct" {
  type        = string
  description = "Service account email for invoking Cloud Function to update App Engine firewall rules"
}

variable "load_gcs_data_cf_svc_acct" {
  type        = string
  description = "Service account email for dynamically starting a (cityblock-orchestration) Cloud Composer DAG from a (cityblock-data) Cloud Function"
}
