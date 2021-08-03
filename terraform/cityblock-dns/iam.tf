resource "google_project_iam_member" "owner" {
  project = "cityblock-dns"
  role    = "roles/owner"
  member  = "group:eng-all@cityblock.com"
}

resource "google_project_iam_member" "dns-admin" {
  project = "cityblock-dns"
  role    = "roles/dns.admin"
  member  = "user:jan.almanzar@cityblock.com"
}
