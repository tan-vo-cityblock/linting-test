module "alina_schnake_mahl_personal_project" {
  source          = "./base"
  person_name     = "Alina Schnake-Mahl"
  person_email    = "alina@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
