module "jordan_hartfiel_personal_project" {
  source          = "./base"
  person_name     = "Jordan Hartfiel"
  person_email    = "jordan.hartfiel@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
