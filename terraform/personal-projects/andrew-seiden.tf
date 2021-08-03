module "andrew_seiden_personal_project" {
  source          = "./base"
  person_name     = "Andy Seiden"
  person_email    = "andrew.seiden@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
