module "gerardo_sierra_personal_project" {
  source          = "./base"
  person_name     = "Gerardo Sierra"
  person_email    = "gerardo.sierra@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
