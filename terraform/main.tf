locals {
  prod_gke_config_context_cluster      = "gke_cityblock-orchestration_us-east4-a_us-east4-prod-airflow-d4e023bb-gke"
  test_prod_gke_config_context_cluster = "gke_cityblock-orchestration_us-east4-a_us-east4-cityblock-composer-cdc9ecc8-gke"
}

provider "kubernetes" {
  version                = "~> 1.6.2"
  config_context_cluster = var.primary_cluster
}

// TODO need to make this automated/derived for the config_context_cluster value
provider "kubernetes" {
  alias                  = "prod-airflow"
  version                = "~> 1.6.2"
  config_context_cluster = local.prod_gke_config_context_cluster
}

provider "google" {
  version = "~> 3.44.0"
}

provider "google-beta" {
  version = "~> 3.44.0"
}

module "emblem-data" {
  source                              = "./emblem-data"
  orchestration_service_account       = module.cityblock-orchestration.orchestration_service_account
  payer_suspect_service_account_email = module.cityblock-orchestration.payer_suspect_service_account_email
  able_health_service_account         = module.cityblock-orchestration.able_health_service_account
}

module "connecticare-data" {
  source                              = "./connecticare-data"
  orchestration_service_account       = module.cityblock-orchestration.orchestration_service_account
  payer_suspect_service_account_email = module.cityblock-orchestration.payer_suspect_service_account_email
  able_health_service_account         = module.cityblock-orchestration.able_health_service_account
}

module "tufts-data" {
  source                        = "./tufts-data"
  orchestration_service_account = module.cityblock-orchestration.orchestration_service_account
  able_health_service_account   = module.cityblock-orchestration.able_health_service_account
}

module "commons-production" {
  source          = "./commons-production"
  billing_account = var.billing_account
}

module "commons-staging" {
  source          = "./commons-staging"
  billing_account = var.billing_account
}

module "cityblock-data" {
  source                                = "./cityblock-data"
  pagerduty_api_key                     = var.pagerduty_api_key
  platform_bot_slack_secret             = var.platform_bot_slack_secret
  emblem_claims_dataset                 = module.emblem-data.claims_dataset
  legacy_emblem_data_project            = module.emblem-data.partner_project_production
  commons_prod_account                  = module.commons-production.commons_service_account_email
  commons_staging_account               = module.commons-staging.commons_staging_svc_acct_email
  orchestration_service_account         = module.cityblock-orchestration.orchestration_service_account
  able_health_service_account           = module.cityblock-orchestration.able_health_service_account
  qreviews_service_account              = module.cityblock-orchestration.qreviews_service_account
  patient_eligibilities_service_account = module.cityblock-orchestration.patient_eligibilities_service_account
  payer_suspect_service_account_email   = module.cityblock-orchestration.payer_suspect_service_account_email
  load_emblem_monthly_svc_acct          = module.cityblock-orchestration.load_emblem_monthly_svc_acct
  load_emblem_weekly_svc_acct           = module.cityblock-orchestration.load_emblem_weekly_svc_acct
  load_cci_monthly_svc_acct             = module.cityblock-orchestration.load_cci_monthly_svc_acct
  airflow_ae_cf_invoker_svc_acct        = module.cityblock-orchestration.airflow_ae_cf_invoker_svc_acct
  load_gcs_data_cf_svc_acct             = module.cityblock-orchestration.load_gcs_data_cf_svc_acct

}

module "staging-cityblock-data" {
  source = "./staging-cityblock-data"
}

module "personal-projects" {
  source          = "./personal-projects"
  billing_account = var.billing_account
}

module "cityblock-orchestration" {
  source           = "./cityblock-orchestration"
  billing          = var.billing_account
  sendgrid_api_key = var.sendgrid_api_key
  providers = {
    kubernetes = "kubernetes.prod-airflow"
  }
  tanjin_compute_service_account = module.personal-projects.tanjin_compute_service_account
}

module "cityblock-cold-storage" {
  source          = "./cityblock-cold-storage"
  billing_account = var.billing_account
}

module "cityblock-dns" {
  source = "./cityblock-dns"
}

terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/state"
  }
}
