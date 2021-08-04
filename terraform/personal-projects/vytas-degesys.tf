module "vytas_degesys_personal_project" {
  source          = "./base"
  person_name     = "Vytas Degesys"
  person_email    = "vytas.degesys@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}