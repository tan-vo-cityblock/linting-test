module "dan_dewald_personal_project" {
  source          = "./base"
  person_name     = "Dan DeWald"
  person_email    = "dan@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}

resource "google_project_iam_member" "cityblock_development_service_account" {
  project = "${module.dan_dewald_personal_project.personal_project_id}"
  role    = "roles/bigquery.user"
  member  = "serviceAccount:cityblock-development@cityblock-development.iam.gserviceaccount.com"
}
