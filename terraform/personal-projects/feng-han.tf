module "feng_han_personal_project" {
    source          = "./base"
    person_name     = "Feng Han"
    person_email    = "feng.han@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
