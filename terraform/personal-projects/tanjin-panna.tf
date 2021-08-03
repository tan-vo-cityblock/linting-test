module "tanjin_panna_personal_project" {
  source          = "./base"
  person_name     = "Tanjin Panna"
  person_email    = "tanjin.panna@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}

data "google_compute_default_service_account" "tp_default" {
  project = "cbh-tanjin-panna"
}

output "tanjin_compute_service_account" {
  description = "Service account that is used for example scio DAG"
  value       = data.google_compute_default_service_account.tp_default
}
