resource "google_pubsub_topic" "multiplexer" {
  name = "multiplexer"
  project = "personal-project-name"

  labels = {
    env = "development"
  }

  message_storage_policy {
    allowed_persistence_regions = [
      "us-east1",
    ]
  }
}