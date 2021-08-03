provider "kubernetes" {
  alias                  = "cityblock-orchestration-prod"
  version                = "~> 1.11.1" // TODO: Update version everywhere to be consistent with this (latest).
  config_context_cluster = data.terraform_remote_state.cbh_root_ref.outputs.prod_gke_config_context_cluster
}

module "sre_project" {
  source     = "../src/resource/project"
  name       = "Cityblock Reliability Ops"
  project_id = "cbh-sre"
  api_services = [
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "bigquery.googleapis.com"
  ]
}

module "cityblock_data_project_ref" {
  source = "../src/data/project"
  project_id = "cityblock-data"
}
