module "caitlin_oconnell_personal_project" {
  source          = "./base"
  person_name     = "Caitlin OConnell"
  person_email    = "caitlin.oconnell@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
