resource "google_project_iam_member" "project_owner" {
  project    = var.partner_project_production
  member     = "group:eng-all@cityblock.com"
  role       = "roles/owner"
}

resource "google_project_iam_member" "product-lead" {
  member  = "user:danny.dvinov@cityblock.com"
  role    = "roles/bigquery.dataEditor"
  project = var.partner_project_production
}

resource "google_project_iam_member" "data_team_bq_admin" {
  member  = "group:data-team@cityblock.com"
  role    = "roles/bigquery.dataEditor"
  project = var.partner_project_production
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = var.partner_project_production
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:actuary@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}",
    "serviceAccount:${module.development_dataflow_svc_acct.email}"
  ]
}
