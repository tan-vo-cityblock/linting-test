module "katherine_fetscher_personal_project" {
  source          = "./base"
  person_name     = "Katherine Fetscher"
  person_email    = "katherine@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}