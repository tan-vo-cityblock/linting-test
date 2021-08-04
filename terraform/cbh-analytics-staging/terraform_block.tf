terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-analytics-staging"
  }
  required_version = "0.12.8"
}
