module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.partner_integrations.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:data-team@cityblock.com",
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}"
  ]
}
