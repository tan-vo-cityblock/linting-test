output "orchestration_service_account" {
  description = "Service account that non-k8s DAG operators utilize by default in Cloud Composer"
  value       = data.google_compute_default_service_account.default
}

output "able_health_service_account" {
  description = "Service account for Able Health workflows"
  value       = module.svc_acct_able_health.service_account_email
}

output "proxy_vm_static_ip" {
  value = google_compute_instance.proxy_url_prod.network_interface.0.access_config.0.nat_ip
}

output "qreviews_service_account" {
  description = "Service account for Q-Reviews workflows"
  value = module.svc_acct_q_reviews.email
}

output "patient_eligibilities_service_account" {
  description = "Service account for updating patient eligibilities"
  value = module.svc_acct_patient_eligibilities.email
}

output "payer_suspect_service_account_email" {
  description = "Service acccount email for payer suspect codes"
  value = module.svc_acct_payer_suspect_service.email
}

output "load_emblem_monthly_svc_acct" {
  value = module.svc_acct_load_monthly_data_emblem.service_account_email
}

output "load_emblem_weekly_svc_acct" {
  value = module.svc_acct_load_weekly_pbm_emblem.service_account_email
}

output "load_cci_monthly_svc_acct" {
  value = module.svc_acct_load_monthly_data_connecticare.service_account_email
}

output "airflow_ae_cf_invoker_svc_acct" {
  value = module.airflow_ae_firewall_update_secret.email
}
output "load_gcs_data_cf_svc_acct" {
  value = module.load_gcs_data_cf_svc_acct.email
}
