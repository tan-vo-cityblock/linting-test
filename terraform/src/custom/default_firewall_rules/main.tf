variable "project_id" {
  type        = "string"
  description = "project id where firewall rules will be applied"
}

locals {
  is_staging = length(regexall("staging", var.project_id)) > 0
  is_prod    = length(regexall("prod", var.project_id)) > 0
  environ    = local.is_prod ? "prod" : "staging"
}

resource "google_app_engine_firewall_rule" "blacklist" {
  project      = var.project_id
  priority     = 2147483646
  action       = "DENY"
  source_range = "*"
}

resource "google_app_engine_firewall_rule" "commons_prod_1" {
  project      = var.project_id
  priority     = 1
  action       = local.is_prod ? "ALLOW" : "DENY"
  source_range = "34.234.191.214"
}

resource "google_app_engine_firewall_rule" "commons_prod_2" {
  project      = var.project_id
  priority     = 2
  action       = local.is_prod ? "ALLOW" : "DENY"
  source_range = "54.85.156.227"
}

resource "google_app_engine_firewall_rule" "gcp_us_central1" {
  project      = var.project_id
  priority     = 3
  action       = local.is_prod || local.is_staging ? "ALLOW" : "DENY"
  source_range = "10.128.0.0/20"
}

resource "google_app_engine_firewall_rule" "cbh_dumbo" {
  project      = var.project_id
  priority     = 4
  action       = local.is_staging || local.is_prod ? "ALLOW" : "DENY"
  source_range = "68.160.220.179"
}

resource "google_app_engine_firewall_rule" "commons_staging_1" {
  project      = var.project_id
  priority     = 5
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "52.73.142.147"
}


resource "google_app_engine_firewall_rule" "commons_staging_2" {
  project      = var.project_id
  priority     = 6
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "52.45.148.202"
}


resource "google_app_engine_firewall_rule" "commons_staging_3" {
  project      = var.project_id
  priority     = 7
  action       = local.is_staging ? "ALLOW" : "DENY"
  source_range = "3.213.230.147"
}
