module "jaclyn_testani_personal_project" {
    source          = "./base"
    person_name     = "Jaclyn Testani"
    person_email    = "jaclyn.testani@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
