module "cotiviti_svc_acct" {
  source      = "../src/resource/service_account"
  project_id  = module.data_science_project.project_id
  account_id  = "cotiviti"
  description = "Cotiviti related workloads"
}
