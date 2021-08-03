terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-carefirst-data"
  }
  required_version = "0.12.8"
}
