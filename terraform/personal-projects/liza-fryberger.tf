module "liza_fryberger_personal_project" {
  source          = "./base"
  person_name     = "Liza Fryberger"
  person_email    = "liza.fryberger@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
