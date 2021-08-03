terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-db-mirror-prod"
  }
  required_version = "0.12.8"
}
