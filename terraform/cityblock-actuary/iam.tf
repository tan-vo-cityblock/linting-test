resource "google_project_iam_binding" "project-editors" {
  project = google_project.actuary.project_id
  members = [
    "group:actuary@cityblock.com",
    "user:friederike.schuur@cityblock.com",
    "user:andrew.seiden@cityblock.com",
  ]
  role = "roles/editor"
}

resource "google_project_iam_member" "owner" {
  project = google_project.actuary.project_id
  member  = "group:eng-all@cityblock.com"
  role    = "roles/owner"
}

module "bq_data_editors" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.actuary.project_id
  role       = "roles/bigquery.dataEditor"
  members = [
    "user:aalap.patel@cityblock.com"
  ]
}

module "bq_data_viewers" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.actuary.project_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "group:data-team@cityblock.com",
    "serviceAccount:looker-demo@cityblock-data.iam.gserviceaccount.com",
    "serviceAccount:${module.dbt_staging_svc_acct.email}",
    "serviceAccount:${module.dbt_prod_svc_acct.email}"
  ]
}

# grants the ability to see saved queries: https://cloud.google.com/bigquery/docs/saving-sharing-queries#permissions
module "bq_users" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.actuary.project_id
  role       = "roles/bigquery.user"
  members = [
    "group:bq-data-access@cityblock.com",
    "serviceAccount:${module.dbt_prod_svc_acct.email}"
  ]
}

module "bq_admin" {
  source     = "../src/custom/project_iam_access"
  project_id = google_project.actuary.project_id
  role       = "roles/bigquery.admin"
  members = [
    "group:actuary@cityblock.com"
  ]
}
