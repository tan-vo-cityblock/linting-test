module "ml_labeling_svc_acct_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source              = "../src/custom/svc_acct_key_k8_secret"
  project_id          = module.data_science_project.project_id
  account_id          = "ml-labeling"
  account_description = "Service account for running ML Labeling processes"
}
