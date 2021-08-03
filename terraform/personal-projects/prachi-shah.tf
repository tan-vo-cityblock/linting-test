module "prachi_shah_personal_project" {
  source          = "./base"
  person_name     = "Prachi Shah"
  person_email    = "prachi.shah@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
