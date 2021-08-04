# Silver Claims
resource "google_bigquery_dataset" "silver_claims" {
  dataset_id    = "silver_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Silver Claims"
  description   = "This dataset contains the files that Connecticare sends us each month as part of the Monthly Data Dump"
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
    user_by_email = module.load_monthly_cci_svc_acct.email
  }
  access {
    role          = "READER"
    user_by_email = module.dbt_staging_svc_acct.email
  }
}

# Gold Claims
resource "google_bigquery_dataset" "gold_claims" {
  dataset_id    = "gold_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Gold Claims"
  description   = "This dataset contains Connecticare claims data in our partner-agnostic schema."
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
    user_by_email = module.load_monthly_cci_svc_acct.email
  }
  access {
    role          = "READER"
    user_by_email = module.dbt_staging_svc_acct.email
  }
}

resource "google_bigquery_dataset" "gold_claims_flattened_facets" {
  dataset_id    = "gold_claims_flattened_facets"
  project       = var.partner_project_production
  friendly_name = "Monthly gold claims flattened for general QA and Great Expectations usage."
  description   = "This dataset contains Connecticare Facets gold claims data with claim lines unbundled."
  location      = "US"
  labels = {
    metal = "gold"
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
    user_by_email = module.load_monthly_cci_svc_acct.email
  }
}


# Messages
resource "google_bigquery_dataset" "messages" { # TODO does anyone use these?
  dataset_id    = "messages"
  project       = var.partner_project_production
  friendly_name = "Debug Messages"
  description   = "Log of messages sent to Commons via PubSub"
  location      = "US"
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
}

resource "google_bigquery_dataset" "mmr_mor" {
  dataset_id  = "mmr_mor"
  project     = var.partner_project_production
  description = "Deprecated dataset for storing various data related to MMR/MOR files"
  location    = "US"
  labels = {
    data = "phi"
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
    role          = "WRITER"
    user_by_email = var.orchestration_service_account.email
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
