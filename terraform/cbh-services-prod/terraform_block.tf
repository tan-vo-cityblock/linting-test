terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-services-prod"
  }
  required_version = "0.12.8"
}
