module "owen_tran_personal_project" {
  source          = "./base"
  person_name     = "Owen Tran"
  person_email    = "owen.tran@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
