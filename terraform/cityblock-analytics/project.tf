/*
Cityblock Analytics project contains all relevant resources around the data marts built and maintained by DSA
*/

provider "kubernetes" {
  alias                  = "cityblock-orchestration-prod"
  version                = "~> 1.11.1" // TODO: Update version everywhere to be consistent with this (latest).
  config_context_cluster = data.terraform_remote_state.cbh_root_ref.outputs.prod_gke_config_context_cluster
}

module "cbh_analytics_project" {
  source              = "../src/resource/project"
  project_id          = "cityblock-analytics"
  name                = "Cityblock Analytics"
  app_engine_location = "us-east4"
  api_services = [
    "bigquery.googleapis.com",
    "appengine.googleapis.com",
    "iap.googleapis.com"
  ]
}
