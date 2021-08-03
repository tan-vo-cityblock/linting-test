resource "google_bigquery_dataset" "computed_fields" {
  dataset_id = "computed_fields"
  project    = var.project_staging
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
    role           = "WRITER"
    group_by_email = "data-team@cityblock.com"
  }
}

resource "google_bigquery_dataset" "streaming" {
  dataset_id = "streaming"
  project    = var.project_staging
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

module "pubsub_messages_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "pubsub_messages"
  project    = var.project_staging
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [{ email = module.staging_zendesk_worker_svc_acct.email, role = "WRITER" }]
  description = "Dataset containing published and/or dead PubSub messages."
}

module "zendesk_dataset" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id = "zendesk"
  project    = var.project_staging
  special_group_access = [
    { special_group = "projectWriters", role = "WRITER" },
    { special_group = "projectOwners", role = "OWNER" },
    { special_group = "projectReaders", role = "READER" }
  ]
  user_access = [{ email = module.staging_zendesk_worker_svc_acct.email, role = "WRITER" }]
  description = "Dataset for housing staging/sandbox zendesk related tables"
}
