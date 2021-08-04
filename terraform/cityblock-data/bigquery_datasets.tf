resource "google_bigquery_dataset" "abstractions" {
  dataset_id    = "abstractions"
  friendly_name = "Abstractions"
  description   = "Dataset containing abstract views over patient data"
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
}

resource "google_bigquery_dataset" "looker_abstractions" {
  dataset_id = "looker_abstractions"
  project    = "cityblock-data"
  labels = {
    status = "legacy"
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
}

resource "google_bigquery_dataset" "medical" {
  dataset_id = "medical"
  project    = "cityblock-data"
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
    role          = "READER"
    user_by_email = var.able_health_service_account
  }
}

resource "google_bigquery_dataset" "streaming" {
  dataset_id = "streaming"
  project    = "cityblock-data"
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
}

resource "google_bigquery_dataset" "ablehealth_results" {
  project    = var.partner_project_production
  dataset_id = "ablehealth_results"
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
    user_by_email = var.able_health_service_account
  }
}

module "qreviews_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "qreviews"
  project    = var.partner_project_production
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = var.qreviews_service_account, role = "WRITER" }
  ]
  description = <<EOF
  Dataset containing results of Q-Reviews metrics that are ingested from them as part of a daily job
  EOF
}

module "reports_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "reports"
  project    = var.partner_project_production
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  description = <<EOF
Reports delivered as is to external partners or internal audiences. Example: CM Delegation Reports.

CM timestamped reports: Starting 4/11/2019, reports for the first 15 days of the month will reflect a reporting date of
the end of the prior month, and reports in the rest of the month will reflect a reporting date of the end of the
current month. E.g. 4/11/19 tables reflect 3/31/19 reports, 4/18/19 tables reflect (in-progress) 4/30/19 reports.
EOF
}

module "pubsub_messages_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "pubsub_messages"
  project    = "cityblock-data"
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    { email = module.pubsub_bq_saver_cf_svc_acct.email, role = "WRITER" },
    { email = module.prod_zendesk_worker_svc_acct.email, role = "WRITER" }
  ]
  description = "Dataset containing published and/or dead PubSub messages."
}

module "zendesk_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "zendesk"
  project    = var.partner_project_production
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [{ email = module.prod_zendesk_worker_svc_acct.email, role = "WRITER" }]
  description = "Dataset for housing prod zendesk related tables"
}

module "src_health_home_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "src_health_home"
  project    = var.partner_project_production
  labels = {
    data = "phi"
  }
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [
    {
      email = var.patient_eligibilities_service_account,
      role  = "WRITER"
    },
    {
      email = module.cityblock_data_project_ref.default_compute_engine_service_account_email,
      role  = "WRITER"
    },
    {
      email = "prachi.shah@cityblock.com",
      role  = "WRITER"
    },
    {
      email = "alan.ghosh@cityblock.com",
      role  = "WRITER"
    }
  ]
  description = "Dataset containing the latest patient eligibilities for health home / HARP."
}
