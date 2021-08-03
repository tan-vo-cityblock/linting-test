module "ben_capistrant_personal_project" {
  source          = "./base"
  person_name     = "Ben Capistrant"
  person_email    = "ben.capistrant@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
