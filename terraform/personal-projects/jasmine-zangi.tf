module "jasmine_zangi_personal_project" {
  source          = "./base"
  person_name     = "Jasmine Zangi"
  person_email    = "jasmine.zangi@cityblock.com"
  billing_account = var.billing_account
  folder_id       = google_folder.personal_projects.id
}
