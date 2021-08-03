module "cary_wang_personal_project" {
  source          = "./base"
  person_name     = "Cary Wang"
  person_email    = "cary.wang@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
