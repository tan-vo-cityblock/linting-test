module "alan_inacio_personal_project" {
  source          = "./base"
  person_name     = "Alan Inacio"
  person_email    = "alan.inacio@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
