module "matt_dickstein_personal_project" {
  source          = "./base"
  person_name     = "Matt Dickstein"
  person_email    = "matt.dickstein@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
