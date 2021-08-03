resource "google_cloud_run_service" "health_events_service_prod" {
  name     = "health-events-service-prod"
  location = "us-east1"
  project = "cbh-services-prod"

  template {
    spec {
      containers {
        image = "us.gcr.io/cityblock-data/health-events-prod:latest"

        args = [
          "run",
          "start"
        ]

        command = [
          "npm"
        ]

        env {
          name  = "NODE_ENV"
          value = "production"
        }

        env {
          name  = "DB_NAME"
          value = module.health_events_db.name
        }

        env {
          name  = "DB_PASSWORD"
          value = data.google_secret_manager_secret_version.prod_health_events_service_db_user_password.secret_data
        }

        env {
          name  = "DB_USER"
          value = data.google_secret_manager_secret_version.prod_health_events_service_db_user_name.secret_data
        }

        env {
          name  = "DB_HOST"
          value = "/cloudsql/cbh-services-prod:us-east1:services"
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}
