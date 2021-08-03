module "load_tufts_daily_service_account" {
  source     = "../src/data/service_account"
  project_id = "cityblock-orchestration"
  account_id = "load-daily-tufts"
}

resource "google_bigquery_dataset" "silver_claims" {
  dataset_id    = "silver_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Silver Claims"
  description   = "This dataset contains the files that Tufts sends us each month as part of the Monthly Data Dump"
  location      = "US"
  labels = {
    metal = "silver"
    data  = "phi"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "READER"
    user_by_email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "WRITER"
    user_by_email = module.load_tufts_daily_service_account.email
  }
  access {
    role          = "READER"
    user_by_email = module.dbt_staging_svc_acct.email
  }
}



resource "google_bigquery_dataset" "gold_claims" {
  dataset_id    = "gold_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Gold Claims"
  description   = "This dataset contains Tufts claims data in our partner-agnostic schema."
  location      = "US"
  labels = {
    metal = "gold"
    data  = "phi"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "READER"
    user_by_email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "WRITER"
    user_by_email = "${module.load_tufts_daily_service_account.email}"
  }
  access {
    role          = "READER"
    user_by_email = var.able_health_service_account
  }
  access {
    role          = "READER"
    user_by_email = module.dbt_staging_svc_acct.email
  }
}

resource "google_bigquery_dataset" "gold_claims_incremental" {
  // TODO: we need to import the existing gold claims incremental dataset and rename correctly here
  dataset_id    = "gold_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Incremental Gold Claims"
  description   = "This dataset contains Tufts claims data in our partner-agnostic schema."
  location      = "US"
  labels = {
    metal = "gold"
    data  = "phi"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "READER"
    user_by_email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "WRITER"
    user_by_email = module.load_tufts_daily_service_account.email
  }
}

module "cms_revenue" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id  = "cms_revenue"
  project     = var.partner_project_production
  description = <<EOF
  Dataset for storing MMR and MOR files and related data.
  EOF
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = var.orchestration_service_account.email, role = "WRITER" }
  ]
}
