resource "google_bigquery_dataset" "caresource" {
  dataset_id    = "caresource"
  project       = var.project_id
  friendly_name = "CareSource"
  description   = "This dataset contains the raw files that CareSource sent at initial stage of partnership"
  location      = "US"
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
    role           = "READER"
    group_by_email = "actuary@cityblock.com"
  }
  access {
    role           = "READER"
    group_by_email = "data-team@cityblock.com"
  }
}

resource "google_bigquery_dataset" "highmark" {
  dataset_id    = "highmark"
  project       = var.project_id
  friendly_name = "Highmark"
  description   = "This dataset contains the raw files that Highmark sent at initial stage of partnership"
  location      = "US"
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
    role           = "READER"
    group_by_email = "actuary@cityblock.com"
  }
  access {
    role          = "READER"
    user_by_email = "danny.dvinov@cityblock.com"
  }
  access {
    role           = "READER"
    group_by_email = "data-team@cityblock.com"
  }
}

resource "google_bigquery_dataset" "ucare" {
  dataset_id    = "ucare"
  project       = var.project_id
  friendly_name = "UCare"
  description   = "This dataset contains the raw files that UCare sent at initial stage of partnership"
  location      = "US"
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
    role           = "READER"
    group_by_email = "actuary@cityblock.com"
  }
  access {
    role           = "READER"
    group_by_email = "data-team@cityblock.com"
  }
}

resource "google_bigquery_dataset" "tufts" {
  dataset_id    = "tufts"
  project       = var.project_id
  friendly_name = "tufts"
  description   = "This dataset contains the raw files that tufts sent at initial stage of partnership"
  location      = "US"
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
    role           = "READER"
    group_by_email = "actuary@cityblock.com"
  }
  access {
    role          = "READER"
    user_by_email = "danny.dvinov@cityblock.com"
  }
  access {
    role          = "WRITER"
    user_by_email = "bobbi.spiegel@cityblock.com"
  }
  access {
    role           = "READER"
    group_by_email = "data-team@cityblock.com"
  }
}

module "cardinal" {
  source     = "../src/resource/bigquery/dataset"
  dataset_id  = "cardinal"
  project     = var.project_id
  description = <<EOF
  Dataset for storing files coming from Cardinal while setting up partnership.
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
    { email = "lucretia.hydell@cityblock.com", role = "READER"},
    { email = "katherine@cityblock.com", role = "READER"},
    { email = "andrew.seiden@cityblock.com", role = "WRITER"},
    { email = "jac.joubert@cityblock.com", role = "READER"},
    { email = "pavel.znoska@cityblock.com", role = "READER"},
    { email = "nathan.sumrall@cityblock.com", role = "READER"}
  ]
}
