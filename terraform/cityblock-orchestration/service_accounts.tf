module "load_gcs_data_cf_svc_acct" {
  source = "../src/data/service_account"
  account_id = "load-gcs-data-cf"
  project_id = module.cityblock_data_project_ref.project_id
}
