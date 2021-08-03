module "pavel_znoska_personal_project" {
  source          = "./base"
  person_name     = "Pavel Znoska"
  person_email    = "pavel.znoska@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}