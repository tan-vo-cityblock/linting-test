module "jac_joubert_personal_project" {
  source          = "./base"
  person_name     = "Jac Joubert"
  person_email    = "jac.joubert@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}

resource "google_project_iam_member" "friede-data-access" {
  project = "${module.jac_joubert_personal_project.personal_project_id}"
  role    = "roles/editor"
  member  = "user:friederike.schuur@cityblock.com"
}

