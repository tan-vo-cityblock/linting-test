module "shezaad_zainulbhai_personal_project" {
  source          = "./base"
  person_name     = "Shezaad Zainulbhai"
  person_email    = "shezaad.zainulbhai@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
