module "alankrita_ghosh_personal_project" {
  source          = "./base"
  person_name     = "Alankrita Ghosh"
  person_email    = "alan.ghosh@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
