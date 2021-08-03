terraform {
  required_version = "0.12.8"
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-git"
  }
}
