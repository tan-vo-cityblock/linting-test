module "rachel_hong_personal_project" {
  source          = "./base"
  person_name     = "Rachel Hong"
  person_email    = "rachel.hong@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
