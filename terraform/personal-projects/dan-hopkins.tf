module "dan_hopkins_personal_project" {
  source          = "./base"
  person_name     = "Dan Hopkins"
  person_email    = "dan.hopkins@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}

