module "nathan_matare_personal_project" {
    source          = "./base"
    person_name     = "Nathan Matare"
    person_email    = "nathan.matare@cityblock.com"
    billing_account = "${var.billing_account}"
    folder_id       = "${google_folder.personal_projects.id}"
}
