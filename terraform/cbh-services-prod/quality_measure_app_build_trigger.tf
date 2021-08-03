module "quality_measure_service_app_prod" {
  source              = "../src/custom/quality_measure_service"
  project_id          = module.services_prod_project.project_id
  cloud_sql_instance  = module.services_postgres_instance.connection_name
  deployment_tag_name = "^quality-measure-service-(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)$"
}
