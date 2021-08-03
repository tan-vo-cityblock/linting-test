terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-member-service-prod"
  }
  required_version = "0.12.8"
}
