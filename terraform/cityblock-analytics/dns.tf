data "google_dns_managed_zone" "cityblock_dot_com" {
  project = "cityblock-dns"
  name    = "cityblock"
}

resource "google_dns_record_set" "prod_url_a_record" {
  project      = data.google_dns_managed_zone.cityblock_dot_com.project
  managed_zone = data.google_dns_managed_zone.cityblock_dot_com.name
  name         = "data-docs.${data.google_dns_managed_zone.cityblock_dot_com.dns_name}"
  rrdatas = [
    "216.239.32.21",
    "216.239.34.21",
    "216.239.36.21",
    "216.239.38.21"
  ]
  ttl  = 300
  type = "A"
}

resource "google_dns_record_set" "prod_url_aaa_record" {
  project      = data.google_dns_managed_zone.cityblock_dot_com.project
  managed_zone = data.google_dns_managed_zone.cityblock_dot_com.name
  name         = "data-docs.${data.google_dns_managed_zone.cityblock_dot_com.dns_name}"
  rrdatas = [
    "2001:4860:4802:32::15",
    "2001:4860:4802:34::15",
    "2001:4860:4802:36::15",
    "2001:4860:4802:38::15"
  ]
  ttl  = 300
  type = "AAAA"
}

resource "google_dns_record_set" "prod_url_cname_record" {
  project      = data.google_dns_managed_zone.cityblock_dot_com.project
  managed_zone = data.google_dns_managed_zone.cityblock_dot_com.name
  name         = "www.data-docs.${data.google_dns_managed_zone.cityblock_dot_com.dns_name}"
  rrdatas = [
    "ghs.googlehosted.com.",
  ]
  ttl  = 300
  type = "CNAME"
}
