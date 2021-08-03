module "david_yoon_personal_project" {
  source          = "./base"
  person_name     = "David Yoon"
  person_email    = "david.yoon@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}