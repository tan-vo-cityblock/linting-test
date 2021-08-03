resource "google_project_service" "personal_project_bigquery" {
  project                    = var.project_id
  service                    = "bigquery.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_bigquery_dataset" "ephemeral_airflow" {
  project                     = var.project_id
  dataset_id                  = "ephemeral_airflow"
  description                 = "A location to store tables temporarily while they are used by intermediate DAG steps"
  default_table_expiration_ms = 86400000

  access {
    role           = "OWNER"
    group_by_email = "gcp-admins@cityblock.com"
  }

  access {
    role          = "WRITER"
    user_by_email = data.google_compute_default_service_account.default.email
  }
}
