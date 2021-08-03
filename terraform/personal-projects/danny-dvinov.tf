module "danny_dvinov_personal_project" {
  source          = "./base"
  person_name     = "Danny Dvinov"
  person_email    = "danny.dvinov@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}