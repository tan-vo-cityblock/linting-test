module "micheal_ruan_personal_project" {
  source          = "./base"
  person_name     = "Michael Ruan"
  person_email    = "michael.ruan@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
