module "lucretia_hydell_personal_project" {
  source          = "./base"
  person_name     = "Lucretia Hydell"
  person_email    = "lucretia.hydell@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
