provider "kubernetes" {
  alias                  = "cityblock-orchestration-prod"
  version                = "~> 1.11.1" // TODO: Update version everywhere to be consistent with this (latest).
  config_context_cluster = data.terraform_remote_state.cbh_root_ref.outputs.prod_gke_config_context_cluster
}

provider "kubernetes" {
  alias                  = "cityblock-orchestration-test"
  version                = "~> 1.11.1" // TODO: Update version everywhere to be consistent with this (latest).
  config_context_cluster = data.terraform_remote_state.cbh_root_ref.outputs.test_gke_config_context_cluster
}

module "cbh_secrets_project" {
  source     = "../src/resource/project"
  project_id = "cbh-secrets"
  name       = "Cityblock Secrets"
  api_services = [
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "storage-api.googleapis.com",
    "secretmanager.googleapis.com"
  ]
}
