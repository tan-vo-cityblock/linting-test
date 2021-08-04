terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-db-mirror-staging"
  }
  required_version = "0.12.8"
}
