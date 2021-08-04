module "services_postgres_instance" {
  source      = "../src/resource/cloudsql/instance"
  project_id  = module.services_prod_project.project_id
  name        = "services"
  enable_logs = false
  labels = {
    data = "phi"
  }
  machine_type = "db-custom-2-3840"
}
