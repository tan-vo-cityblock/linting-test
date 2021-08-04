module "quality_measure_service_app_staging" {
  source                 = "../src/custom/quality_measure_service"
  project_id             = module.services_staging_project.project_id
  cloud_sql_instance     = module.services_postgres_instance.connection_name
  deployment_branch_name = "^master$"
}
