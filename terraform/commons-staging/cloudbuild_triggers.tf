resource "google_cloudbuild_trigger" "commons_build_staging" {
  name        = "commons-build-staging"
  description = "Build and deploy Commons Staging"
  project     = module.cityblock_data_project_ref.project_id
  trigger_template {
    project_id  = module.cityblock_data_project_ref.project_id
    repo_name   = "github_cityblock_commons"
    branch_name = "^master$"
  }
  substitutions = {
    _COMMONS_IMAGE = "us.gcr.io/${module.cityblock_data_project_ref.project_id}/commons"
  }
  ignored_files = [
    ".circleci/**",
  ]
  filename       = "cloudbuild/staging.yaml"
}

resource "google_cloudbuild_trigger" "k6_build" {
  name        = "commons-k6-staging"
  description = "Build and deploy k6 Image"
  project     = module.cityblock_data_project_ref.project_id
  trigger_template {
    project_id  = module.cityblock_data_project_ref.project_id
    repo_name   = "github_cityblock_commons"
    branch_name = "^master$"
  }
  substitutions = {
    _K6_IMAGE = "us.gcr.io/${module.cityblock_data_project_ref.project_id}/commons-k6"
  }
  included_files = [
    "load-test/**",
  ]
  filename       = "cloudbuild/k6.yaml"
}
