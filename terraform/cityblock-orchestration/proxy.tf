resource "google_compute_instance" "proxy_url_prod" {
  project                 = var.project_id
  name                    = "proxy-airflow-url-prod"
  machine_type            = "f1-micro"
  zone                    = "us-central1-a"
  metadata_startup_script = <<EOF
      sudo apt-get update && sudo apt-get install apache2 -y &&
      sudo bash -c 'echo "Redirect 301 / ${google_composer_environment.prod_cluster.config[0].airflow_uri}" >> /etc/apache2/apache2.conf' &&
      sudo /etc/init.d/apache2 start
  EOF
  tags                    = ["http-server", "https-server"]

  boot_disk {
    initialize_params {
      image = "debian-9-stretch-v20191210"
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephermetal IP
    }
  }
}
