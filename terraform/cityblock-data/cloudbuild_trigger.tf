resource "google_cloudbuild_trigger" "elation-hook-trigger" {
  description = "Auto-deploy elation-hook"
  project     = var.partner_project_production
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = var.mixer_repo
    branch_name = "^master$"
  }

  substitutions = {
    _FXN_NAME   = google_cloudfunctions_function.elationHookEventReceiver.name
    _FXN_SOURCE = google_cloudfunctions_function.elationHookEventReceiver.source_repository.0.url
    _FXN_REGION = google_cloudfunctions_function.elationHookEventReceiver.region

  }
  included_files = ["cloud_functions/elation_hook/*"]
  filename       = "cloud_functions/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "container-updates" {
  description = "Build and push container updates to GCR"
  project     = var.partner_project_production
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = var.mixer_repo
    branch_name = "^master$"
  }
  included_files = ["containers/**"]
  filename       = "containers/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "scio-application" {
  description = "Builds and push Scio application"
  project     = var.partner_project_production
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = var.mixer_repo
    branch_name = "^master$"
  }
  included_files = ["scio-jobs/**"]
  filename       = "scio-jobs/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "script-image" {
  description = "Build and push scripts (ruby bundle)"
  project     = var.partner_project_production
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = var.mixer_repo
    branch_name = "^master$"
  }
  included_files = ["scripts/**"]
  filename       = "scripts/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "dbt_staging_run" {
  provider    = google-beta
  project     = var.partner_project_production
  description = "dbt run for pull requests in staging environment"
  included_files = [
    "dbt/analysis/**",
    "dbt/data/**",
    "dbt/macros/**",
    "dbt/models/**",
    "dbt/scripts/**",
    "dbt/snapshots/**",
    "dbt/tests/**"
  ]
  ignored_files = [
    "**.sh"
  ]

  github {
    owner = "cityblock"
    name  = "mixer"
    pull_request {
      branch          = "^master$"
      comment_control = "COMMENTS_ENABLED"
    }
  }
  filename = "dbt/cloud_build/staging/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "dbt_prod_run" {
  provider    = google-beta
  project     = var.partner_project_production
  description = "dbt run for production"
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = var.mixer_repo
    branch_name = "^master$"
  }
  included_files = [
    "dbt/analysis/**",
    "dbt/data/**",
    "dbt/macros/**",
    "dbt/models/**",
    "dbt/scripts/**",
    "dbt/snapshots/**",
    "dbt/tests/**"
  ]
  ignored_files = [
    "**.sh"
  ]
  filename = "dbt/cloud_build/prod/cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "airflow_ae_firewall_rules" {
  description = "Auto-deploy Airflow App Engine Firewall Rules Service"
  project     = var.partner_project_production
  trigger_template {
    project_id  = var.partner_project_production
    repo_name   = google_sourcerepo_repository.mixer_mirror.name
    branch_name = "^master$"
  }
  build {
    step {
      # release on June 4th 2020
      name = "gcr.io/cloud-builders/gcloud-slim@sha256:317353ab0694b7b25d2c71c7b4639b7b31b8b2e8c055ca3e84a25029372a4b9d"
      args = [
        "functions", "deploy", google_cloudfunctions_function.airflow_ae_firewall_allow.name,
        "--source", google_cloudfunctions_function.airflow_ae_firewall_allow.source_repository.0.url
      ]
      timeout = "120s"
    }
  }
  included_files = ["cloud_functions/airflow_ae_firewall_allow/*"]
  ignored_files  = ["**.md"]
}

