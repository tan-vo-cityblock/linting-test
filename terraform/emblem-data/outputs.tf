output "claims_dataset" {
  value = "${google_bigquery_dataset.monthly_claims.dataset_id}"
}

output "partner_project_production" {
  value = "${var.partner_project_production}"
}
