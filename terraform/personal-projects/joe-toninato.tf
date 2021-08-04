module "joe_toninato_personal_project" {
    source          = "./base"
    person_name     = "Joe Toninato"
    person_email    = "joseph.toninato@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
