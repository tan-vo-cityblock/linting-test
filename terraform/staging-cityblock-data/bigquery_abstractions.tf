resource "google_bigquery_dataset" "abstractions" {
  dataset_id    = "abstractions"
  friendly_name = "Abstractions"
  description   = "Dataset containing abstract views over patient data"

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

module "diagnoses_abstraction" {
  source      = "../view"
  view_sql    = "${file("./staging-cityblock-data/sql/abstractions/diagnoses.sql")}"
  project_id  = "${var.project_staging}" # staging-cityblock-data
  dataset_id  = "${google_bigquery_dataset.abstractions.dataset_id}"
  view_name   = "diagnoses"
  description = "Diagnoses from partner claims, problem list, CCD, and HIE"
}
