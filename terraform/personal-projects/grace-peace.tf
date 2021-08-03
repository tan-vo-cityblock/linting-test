module "grace_peace_personal_project" {
    source          = "./base"
    person_name     = "Grace Peace"
    person_email    = "grace.peace@cityblock.com"
    billing_account = var.billing_account
    folder_id       = google_folder.personal_projects.id
}
