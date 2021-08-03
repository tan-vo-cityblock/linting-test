module "dax_paramanathan_personal_project" {
  source          = "./base"
  person_name     = "Dax Paramanathan"
  person_email    = "dax@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
