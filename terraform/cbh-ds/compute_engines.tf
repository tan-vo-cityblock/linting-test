resource "google_compute_instance" "cotiviti_dxcg" {
  project      = module.data_science_project.project_id
  name         = "cotiviti-dxcg"
  machine_type = "n2-standard-4"
  zone         = "us-central1-a"
  labels = {
    data = "phi"
  }
  boot_disk {
    device_name = "cotiviti-dxcg"
    mode        = "READ_WRITE"
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-focal-v20200907"
      size  = 30
      type  = "pd-standard"
    }
  }
  network_interface {
    network = "default"
    access_config {
      // Ephemeral IP
    }
  }
  service_account {
    email  = module.cotiviti_svc_acct.email
    scopes = ["cloud-platform"]
  }
  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }
}
