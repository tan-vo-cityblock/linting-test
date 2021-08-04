module "kms_project" {
  source     = "../src/resource/project"
  project_id = "cbh-kms"
  name       = "Cityblock Cloud KMS"
  api_services = [
    "cloudkms.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com"
  ]
}
