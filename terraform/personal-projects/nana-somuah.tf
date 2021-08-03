module "nana_somuah_personal_project" {
  source          = "./base"
  person_name     = "Nana Somuah"
  person_email    = "nana.somuah@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
