resource "google_cloudbuild_trigger" "data_docs_trigger" {
  name        = "data-docs-deploy-app"
  description = "Deploy Data Docs app from source"
  project     = module.cityblock_data_project_ref.project_id
  trigger_template {
    project_id  = module.cityblock_data_project_ref.project_id
    repo_name   = "github_cityblock_mixer"
    branch_name = "^master$"
  }
  included_files = ["services/data_docs/**"]
  filename       = "services/data_docs/cloudbuild.yaml"
}

//IAM perms for cityblock-data cloudbuild service account to build and deploy the app engine data_docs app.
module "data_docs_cloudbuild_iam" {
  source                = "../src/custom/cloudbuild_app_engine_iam"
  app_engine_project_id = module.cbh_analytics_project.project_id
  grant_cloudsql_perms  = false
}
