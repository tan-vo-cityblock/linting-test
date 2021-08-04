module "marianne_hoogeveen_personal_project" {
  source          = "./base"
  person_name     = "Marianne Hoogeveen"
  person_email    = "marianne.hoogeveen@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
