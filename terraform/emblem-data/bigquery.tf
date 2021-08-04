# Monthly Claims
resource "google_bigquery_dataset" "monthly_claims" {
  dataset_id    = "monthly_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Claims"
  description   = "This dataset contains the files that Emblem sends us each month as part of the Monthly Data Dump"
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
    user_by_email = "healthix-transfer@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "READER"
    user_by_email = "patient-index-editor@appspot.gserviceaccount.com"
  }
  access {
    role          = "READER"
    user_by_email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "READER"
    user_by_email = var.orchestration_service_account.email
  }
  access {
    role          = "WRITER"
    user_by_email = module.load_monthly_emblem_svc_acct.email
  }
}

# Silver Claims
resource "google_bigquery_dataset" "silver_claims" {
  dataset_id    = "silver_claims"
  project       = var.partner_project_production
  friendly_name = "Silver Claims"
  description   = "This dataset contains the files that Emblem sends us each month as part of the monthly data dump, with cityblock-generated UUID's added."
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
    user_by_email = "healthix-transfer@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "READER"
    user_by_email = "patient-index-editor@appspot.gserviceaccount.com"
  }
  access {
    role          = "READER"
    user_by_email = "dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
  }
  access {
    role          = "WRITER"
    user_by_email = module.load_monthly_emblem_svc_acct.email
  }
}

# Gold Claims
resource "google_bigquery_dataset" "gold_claims" {
  dataset_id    = "gold_claims"
  project       = var.partner_project_production
  friendly_name = "Monthly Gold Claims"
  description   = "This dataset contains Emblem claims data in our partner-agnostic schema."
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
    role          = "READER"
    user_by_email = var.able_health_service_account
  }
  access {
    role          = "WRITER"
    user_by_email = module.load_monthly_emblem_svc_acct.email
  }
}

# MMR/MOR
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

module "expense_reports" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id  = "expense_reports"
  project     = var.partner_project_production
  description = <<EOF
  Dataset for storing MMR and MOR files and related data.
  EOF
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = "jac.joubert@cityblock.com", role = "READER"},
    { email = "pavel.znoska@cityblock.com", role = "READER"},
    { email = "nathan.sumrall@cityblock.com", role = "READER"},
    { email = "lucretia.hydell@cityblock.com", role = "READER"}
  ]
}
