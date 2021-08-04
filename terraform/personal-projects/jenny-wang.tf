module "jenny_wang_personal_project" {
  source          = "./base"
  person_name     = "Jenny Wang"
  person_email    = "jenny.wang@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
