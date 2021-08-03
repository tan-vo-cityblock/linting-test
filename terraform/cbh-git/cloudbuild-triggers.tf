locals {
  ge_dir = "data_infra/great_expectations"
}
resource "google_cloudbuild_trigger" "gitlab_ge_container_trigger" {
  name        = "gitlab-great-expectations-trigger"
  description = "Build and push ge container updates to GCR"
  project     = module.git_project.project_id
  trigger_template {
    project_id  = module.git_project.project_id
    repo_name   = google_sourcerepo_repository.picasso_mirror.name
    branch_name = "^master$"
  }
  substitutions = {
    _GE_IMAGE = "us.gcr.io/${module.git_project.project_id}/great_expectations"
  }
  ignored_files = [
    "${local.ge_dir}/notebooks_starters/**",
    "${local.ge_dir}/uncommitted/**",
    "${local.ge_dir}/**/README.md",
    "${local.ge_dir}/**/batches_for_create_jinja.json",
    "${local.ge_dir}/**/create_expectations.py",
    "${local.ge_dir}/**/expectations_params.json",
    "${local.ge_dir}/**/requirements-dev.txt"
  ]
  included_files = ["${local.ge_dir}/**"]
  filename       = "${local.ge_dir}/cloudbuild.yml"
}
