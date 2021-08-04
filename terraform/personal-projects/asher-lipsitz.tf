module "asher_lipsitz_personal_project" {
    source          = "./base"
    person_name     = "Asher Lipsitz"
    person_email    = "asher.lipsitz@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
