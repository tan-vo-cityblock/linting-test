module "luming_hao_personal_project" {
  source          = "./base"
  person_name     = "Luming Hao"
  person_email    = "luming.hao@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}