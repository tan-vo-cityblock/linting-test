module "tf_svc_load_monthly_cci_ref" {
  source     = "../src/data/service_account"
  project_id = "cityblock-orchestration"
  account_id = "tf-svc-load-monthly-cci"
}
