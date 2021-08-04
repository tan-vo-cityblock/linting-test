module "craig_spaeth_personal_project" {
  source          = "./base"
  person_name     = "Craig Spaeth"
  person_email    = "craig.spaeth@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
