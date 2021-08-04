data "google_dns_managed_zone" "cityblock_dot_com" {
  project = "cityblock-dns"
  name    = "cityblock"
}

resource "google_dns_record_set" "prod_url_a_record" {
  project      = data.google_dns_managed_zone.cityblock_dot_com.project
  managed_zone = data.google_dns_managed_zone.cityblock_dot_com.name
  name         = "airflow-prod.${data.google_dns_managed_zone.cityblock_dot_com.dns_name}"
  rrdatas      = [google_compute_instance.proxy_url_prod.network_interface.0.access_config.0.nat_ip]
  ttl          = 300
  type         = "A"
}
