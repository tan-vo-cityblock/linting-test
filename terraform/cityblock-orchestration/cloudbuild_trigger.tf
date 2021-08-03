resource "google_cloudbuild_trigger" "sync_dags" {
  description = "Sync DAGs and plugins from mixer to Cloud Composer"
  project     = var.project_id
  trigger_template {
    project_id  = var.project_id
    repo_name   = "github_cityblock_mixer"
    branch_name = "^master$"
  }

  substitutions = {
    _COMPOSER_BUCKET = replace(google_composer_environment.prod_cluster.config.0.dag_gcs_prefix, "/dags", "")
  }
  included_files = [
    "cloud_composer/dags/*.py",
    "cloud_composer/plugins/hooks/*.py",
    "cloud_composer/plugins/operators/*.py",
  ]
  filename = "cloud_composer/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "dbt-image" {
  description = "Build and push dbt project"
  project     = var.project_id
  trigger_template {
    project_id  = var.project_id
    repo_name   = "github_cityblock_mixer"
    branch_name = "^master$"
  }
  included_files = ["dbt/**"]
  filename       = "dbt/cloud_build/prod/cloudbuild.docker.yaml"
}

