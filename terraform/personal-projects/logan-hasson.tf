module "logan_hasson_personal_project" {
  source          = "./base"
  person_name     = "Logan Hasson"
  person_email    = "logan@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}