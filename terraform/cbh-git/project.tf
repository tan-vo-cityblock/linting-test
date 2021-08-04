module "git_project" {
  source     = "../src/resource/project"
  name       = "Cityblock Git"
  project_id = "cbh-git"
  api_services = [
    "compute.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "cloudtrace.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "sourcerepo.googleapis.com",
    "cloudbuild.googleapis.com"
  ]
}
