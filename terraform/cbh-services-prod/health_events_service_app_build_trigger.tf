module "health_events_service_app_prod" {
  source              = "../src/custom/health-events-service"
  project_id          = module.services_prod_project.project_id
  cloud_sql_instance  = module.services_postgres_instance.connection_name
  deployment_tag_name = "^health-events-service-(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)$"
  database_name       = module.health_events_db.name
}
