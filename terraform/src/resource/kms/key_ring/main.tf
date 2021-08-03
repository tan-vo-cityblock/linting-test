output "key_ring_self_link" {
  value       = google_kms_key_ring.key_ring.self_link
  description = "The self link of the created KeyRing in the format projects/{project}/locations/{location}/keyRings/{name}"
  sensitive   = true
}

output "name" {
  value       = google_kms_key_ring.key_ring.name
  description = "The name of the key ring. Used in url for for corresponding ciphertext stored in GCS and as args for decrypting in cloudbuild resources"
  sensitive   = true
}

variable "name" {
  type        = "string"
  description = "Name of key ring"
}

variable "crypto_key_decrypters" {
  type        = list(string)
  description = "List of user emails that can decrypt all keys on a key ring"
}

resource "google_kms_key_ring" "key_ring" {
  project  = "cbh-kms"
  name     = var.name
  location = "us" // A full list of valid locations can be found by running 'gcloud kms locations list'
}

data "google_iam_policy" "key_ring_policy_data" {
  binding {
    role = "roles/cloudkms.admin"

    members = [
      "group:gcp-admins@cityblock.com"
    ]
  }

  binding {
    role = "roles/cloudkms.cryptoKeyEncrypterDecrypter" // Note: I'm not exposing this as a variable just yet until we define better define encrypters in our org. Probably gonna be a process that invovles Charles/his team.

    members = [
      "group:gcp-admins@cityblock.com"
    ]
  }

  binding {
    role = "roles/cloudkms.cryptoKeyDecrypter"

    members = var.crypto_key_decrypters
  }
}

resource "google_kms_key_ring_iam_policy" "key_ring_policy" {
  key_ring_id = google_kms_key_ring.key_ring.id
  policy_data = data.google_iam_policy.key_ring_policy_data.policy_data
}
