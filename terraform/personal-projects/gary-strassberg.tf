module "gary_strassberg_personal_project" {
  source          = "./base"
  person_name     = "Gary Strassberg"
  person_email    = "gary.strassberg@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
