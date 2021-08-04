module "load_cci_service_account" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "tf-svc-load-monthly-cci"
}

module "load_emblem_service_account" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "tf-svc-load-monthly-emblem"
}

module "load_emblem_pbm_service_account" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "tf-svc-load-weekly-pbm-emblem"
}

module "load_tufts_daily_service_account" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "load-daily-tufts"
}

module "svc_acct_carefirst_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "carefirst-worker"
}

module "svc_acct_cardinal_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "cardinal-worker"
}

module "svc_acct_healthyblue_worker" {
  source     = "../src/data/service_account"
  project_id = module.cityblock_orchestration_project_ref.project_id
  account_id = "healthyblue-worker"
}

module "elation_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "elation-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "redox_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "redox-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "prod_zendesk_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "staging_zendesk_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "staging-zendesk-worker"
  project_id = module.cityblock_orchestration_project_ref.project_id
}

module "great_expectations_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "great-expectations"
  project_id = module.cityblock_analytics_project_ref.project_id
}

module "prod_cureatr_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-cureatr-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "dev_cureatr_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "dev-cureatr-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "prod_healthgorilla_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-healthgorilla-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "dev_healthgorilla_worker_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "dev-healthgorilla-worker"
  project_id = module.cityblock_data_project_ref.project_id
}

module "prod_commons_mirror_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-commons-mirror"
  project_id = module.cbh_db_mirror_prod_ref.project_id
}

module "staging_commons_mirror_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "staging-commons-mirror"
  project_id = module.cbh_db_mirror_staging_ref.project_id
}

module "prod_quality_measure_mirror_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-quality-measure-mirror"
  project_id = module.cbh_db_mirror_prod_ref.project_id
}

module "staging_member_index_mirror_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "staging-member-index-mirror"
  project_id = module.cbh_db_mirror_staging_ref.project_id
}

module "prod_member_index_mirror_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "prod-member-index-mirror"
  project_id = module.cbh_db_mirror_prod_ref.project_id
}

module "payer_suspect_svc_acct" {
  source     = "../src/data/service_account"
  account_id = "process-payer-suspect"
  project_id = module.cityblock_orchestration_project_ref.project_id
}
