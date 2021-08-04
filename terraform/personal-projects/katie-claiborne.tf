module "katie_claiborne_personal_project" {
  source          = "./base"
  person_name     = "Katie Claiborne"
  person_email    = "katie.claiborne@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}