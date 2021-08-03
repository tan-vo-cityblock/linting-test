// DATASET - medical
module "patient_encounters" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = google_bigquery_dataset.medical.dataset_id
  table_id      = "patient_encounters"
  friendly_name = "patient_encounters"
  description   = "Model for storing data related to member medical encounters/visits from Redox and Elation ehrs."
  schema        = file("cityblock-data/bigquery_schemas/patient_encounters.json")
}

// DATASET - qreviews
module "qreviews_results" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = module.qreviews_dataset.dataset_id
  table_id      = "qreviews_results"
  friendly_name = "qreviews_results"
  description   = "Stores Q-Review results via the Airflow DAG qreviews_to_cbh_v[x]"
  schema        = file("cityblock-data/bigquery_schemas/qreviews/qreviews_results.json")
}

// DATASET - pubsub_messages
module "dead_letter_table" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = module.pubsub_messages_dataset.dataset_id
  table_id      = "dead_letter"
  friendly_name = "dead_letter"
  description   = "Table that stores Pub/Sub messages sent to dead_letter topic from all topics in cityblock-data"
  schema        = file("cityblock-data/bigquery_schemas/pubsub_messages/generalized_pubsub.json")
}

module "member_demographics_table" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = module.pubsub_messages_dataset.dataset_id
  table_id      = "member_demographics"
  friendly_name = "member_demographics"
  description   = "Table that stores Pub/Sub messages sent to Member Demographics Pub/Sub topic, and eventually to Commons"
  schema        = file("cityblock-data/bigquery_schemas/pubsub_messages/generalized_pubsub.json")
}

// DATASET - ablehealth_results
module "ah_measure_results" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = google_bigquery_dataset.ablehealth_results.dataset_id
  table_id      = "measure_results"
  description   = "All Measure Results for Able Health ingestion data"
  schema        = file("cityblock-data/bigquery_schemas/ablehealth/measure_results.json")
}

module "ah_risk_scores" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = google_bigquery_dataset.ablehealth_results.dataset_id
  table_id      = "risk_scores"
  description   = "All Risk Scores for Able Health ingestion data"
  schema        = file("cityblock-data/bigquery_schemas/ablehealth/risk_scores.json")
}

module "ah_risk_suspects" {
  source        = "../src/resource/bigquery/table"
  project_id    = var.partner_project_production
  dataset_id    = google_bigquery_dataset.ablehealth_results.dataset_id
  table_id      = "risk_suspects"
  description   = "All Risk Suspects for Able Health ingestion data"
  schema        = file("cityblock-data/bigquery_schemas/ablehealth/risk_suspects.json")
}
