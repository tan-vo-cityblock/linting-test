module "legacy_sink_dataset" {
  source     = "../src/resource/bigquery/dataset"
  project    = google_project.commons-staging.id
  dataset_id = "aptible_logs"
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
}

resource "google_logging_project_bucket_config" "aptible_logs_bucket" {
  bucket_id      = "aptible"
  location       = "us-east1"
  project        = "projects/${google_project.commons-staging.id}"
  retention_days = 60
}

resource "google_logging_project_sink" "aptible_bucket_sink" {
  project                = google_project.commons-staging.project_id
  destination            = "logging.googleapis.com/${google_logging_project_bucket_config.aptible_logs_bucket.name}"
  name                   = "commons-staging-logs-sink"
  filter                 = "labels.source=aptible"
  unique_writer_identity = true
}
