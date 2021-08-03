module "mary_edmond_personal_project" {
    source          = "./base"
    person_name     = "Mary Edmond"
    person_email    = "mary.edmond@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
