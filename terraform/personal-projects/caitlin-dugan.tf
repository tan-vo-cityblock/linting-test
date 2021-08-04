module "caitlin_dugan_personal_project" {
  source          = "./base"
  person_name     = "Caitlin Dugan"
  person_email    = "caitlin.dugan@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
