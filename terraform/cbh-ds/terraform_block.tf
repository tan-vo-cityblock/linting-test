terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-data-science"
  }
  required_version = "0.12.8"
}
