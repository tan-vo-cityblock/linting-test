terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/analytics"
  }
  required_version = "0.12.8"
}
