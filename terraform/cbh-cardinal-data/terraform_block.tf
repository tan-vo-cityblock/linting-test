terraform {
  backend "gcs" {
    bucket = "cbh-terraform-state"
    prefix = "terraform/cbh-cardinal-data"
  }
  required_version = "0.12.8"
}
