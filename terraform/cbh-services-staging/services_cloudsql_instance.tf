module "services_postgres_instance" {
  source      = "../src/resource/cloudsql/instance"
  project_id  = module.services_staging_project.project_id
  name        = "services"
  enable_logs = false
}
