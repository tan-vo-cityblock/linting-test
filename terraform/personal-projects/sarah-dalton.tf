module "sarah_dalton_personal_project" {
  source          = "./base"
  person_name     = "Sarah Dalton"
  person_email    = "sarah.dalton@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
