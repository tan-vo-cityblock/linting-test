provider "kubernetes" {
  alias                  = "cityblock-orchestration-prod"
  version                = "~> 1.11.1" // TODO: Update version everywhere to be consistent with this (latest).
  config_context_cluster = data.terraform_remote_state.cbh_root_ref.outputs.prod_gke_config_context_cluster
}
