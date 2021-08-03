module "mohammed_husain_personal_project" {
    source          = "./base"
    person_name     = "Mohammed Husain"
    person_email    = "mohammed.husain@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
