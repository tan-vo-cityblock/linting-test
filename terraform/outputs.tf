output "staging_member_index_svc_acct" {
  value       = module.staging-cityblock-data.member_index_svc_acct
  description = "Service account associated with Cloud SQL Instance for Member Index Staging"
}

output "prod_member_index_svc_acct" {
  value       = module.cityblock-data.member_index_svc_acct
  description = "Service account associated with Cloud SQL Instance for Member Index Production"
}

output "airflow_proxy_ip_prod" {
  value       = module.cityblock-orchestration.proxy_vm_static_ip
  description = "External IP address associated with instance directing to Airflow UI"
}

output "prod_gke_config_context_cluster" {
  value       = local.prod_gke_config_context_cluster
  description = "For use as remote state output when needing to access cluster via the kuberenetes provider"
}

output "test_gke_config_context_cluster" {
  value       = local.test_prod_gke_config_context_cluster
  description = "For use as remote state output when needing to access cluster via the kuberenetes provider"
}
