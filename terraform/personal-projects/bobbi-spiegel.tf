module "bobbi_spiegel_personal_project" {
  source          = "./base"
  person_name     = "Bobbi Spiegel"
  person_email    = "bobbi.spiegel@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
