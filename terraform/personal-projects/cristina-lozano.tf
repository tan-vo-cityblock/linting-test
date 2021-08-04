module "cristina_lozano_personal_project" {
  source          = "./base"
  person_name     = "Cristina Lozano"
  person_email    = "cristina@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
