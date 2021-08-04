#
resource "google_project_iam_member" "k8s_viewer" {
  project = module.cityblock_orchestration_project_ref.project_id
  member  = "serviceAccount:${module.airflow_app_engine_svc_acct.email}"
  role    = "roles/container.viewer"
}

resource "google_project_iam_member" "mem_svc_ae_admin_staging" {
  project = module.cbh_member_service_staging_ref.project_id
  member  = "serviceAccount:${module.airflow_app_engine_svc_acct.email}"
  role    = "roles/appengine.appAdmin"
}

resource "google_project_iam_member" "mem_svc_ae_admin_prod" {
  project = module.cbh_member_service_prod_ref.project_id
  member  = "serviceAccount:${module.airflow_app_engine_svc_acct.email}"
  role    = "roles/appengine.appAdmin"
}

resource "google_project_iam_member" "services_ae_admin_staging" {
  project = module.cbh_services_staging_ref.project_id
  member  = "serviceAccount:${module.airflow_app_engine_svc_acct.email}"
  role    = "roles/appengine.appAdmin"
}

resource "google_project_iam_member" "services_ae_admin_prod" {
  project = module.cbh_services_prod_ref.project_id
  member  = "serviceAccount:${module.airflow_app_engine_svc_acct.email}"
  role    = "roles/appengine.appAdmin"
}
