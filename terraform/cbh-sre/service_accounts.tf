module "sli_metrics_svc_acct" {
  source = "../src/resource/service_account"
  project_id = module.sre_project.project_id
  account_id = "sli-metrics"
}

module "looker_svc_acct" {
  source = "../src/data/service_account"
  project_id = module.cityblock_data_project_ref.project_id
  account_id = "looker-demo"
}
