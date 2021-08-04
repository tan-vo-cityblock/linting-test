module "dataflow_svc_acct" {
  source      = "../src/resource/service_account"
  description = "Service account used for running all Dataflow jobs"
  project_id  = module.development_project.project_id
  account_id  = "dataflow"
}

module "dev_svc_acct" {
  source     = "../src/resource/service_account"
  project_id = module.development_project.project_id
  account_id = "cityblock-development"
}
