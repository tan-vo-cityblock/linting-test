module "lauren_friedmann_personal_project" {
  source          = "./base"
  person_name     = "Lauren Friedmann"
  person_email    = "lauren.friedmann@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
