module "service_account_sftp_drop_trigger" {
  source     = "../../src/data/service_account"
  account_id = "load-gcs-data-cf"
  project_id = module.gcp_project_cityblock_data.project_id
}
