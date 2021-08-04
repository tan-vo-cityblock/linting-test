module "diana_stan_personal_project" {
  source          = "./base"
  person_name     = "Diana Stan"
  person_email    = "diana.stan@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
