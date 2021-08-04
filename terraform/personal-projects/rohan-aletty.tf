module "rohan_aletty_personal_project" {
  source          = "./base"
  person_name     = "Rohan Aletty"
  person_email    = "rohan.aletty@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
