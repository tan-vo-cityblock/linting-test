resource "google_compute_network" "gitlab_network" {
  project                 = var.project_id
  description             = "Network for GitLab Instance"
  name                    = "gitlab-network"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "external_ports_ssl" {
  project = var.project_id
  name    = "gitlab-external-ports"
  network = google_compute_network.gitlab_network.name
  allow {
    protocol = "tcp"
    ports    = var.public_ports_ssl
  }
}

resource "google_compute_address" "external_ip" {
  project = var.project_id
  name    = "gitlab-external-address"
  region  = var.region
}

resource "google_compute_disk" "gitlab_data" {
  project = var.project_id
  name    = "gitlab-data"
  size    = 80
  type    = "pd-ssd"
  zone    = var.zone
}

resource "google_compute_instance" "gitlab_instance" {
  project      = var.project_id
  machine_type = var.machine_type
  name         = var.instance_name
  zone         = var.zone
  tags         = ["gitlab", "http", "https"]
  boot_disk {
    initialize_params {
      image = var.image
    }
    auto_delete = false
  }
  attached_disk {
    source = google_compute_disk.gitlab_data.self_link
  }
  network_interface {
    network = google_compute_network.gitlab_network.self_link
    access_config {
      nat_ip = google_compute_address.external_ip.address
    }
  }
}

resource "google_dns_record_set" "gitlab_instance" {
  project      = var.dns_project
  managed_zone = var.dns_zone
  name         = var.dns_name
  rrdatas      = [google_compute_address.external_ip.address]
  ttl          = 300
  type         = "A"
}

module "gitlab_backup" {
  source     = "../../resource/storage/bucket"
  project_id = var.project_id
  name       = "${var.project_id}-backups"
  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}