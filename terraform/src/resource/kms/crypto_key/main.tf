output "crypto_key_self_link" {
  value       = google_kms_crypto_key.crypto_key.self_link
  description = "The self link of the created CryptoKey. Its format is {{key_ring}}/cryptoKeys/{{name}}."
  sensitive   = true
}

output "name" {
  value       = google_kms_crypto_key.crypto_key.name
  description = "The name of the key"
  sensitive   = true
}

variable "name" {
  type        = "string"
  description = "Name of crypto key. Used in url for for corresponding ciphertext stored in GCS and as args for decrypting in cloudbuild resources"
}

variable "key_ring" {
  type        = "string"
  description = "Self link of key ring where crypto key belongs"
}

variable "purpose_algorithm" {
  type = object({ purpose = string, algorithm = string }) // TODO: Implement check to make sure grouping is valid.

  description = <<EOF
    The immutable purpose of this CryptoKey. The purpose also determines which algorithms are supported for the key's versions.
    Each algorithm defines what parameters must be used for each cryptographic operation
    Possible values for purpose: ENCRYPT_DECRYPT, ASYMMETRIC_SIGN, ASYMMETRIC_DECRYPT.
    For algorithms and more details: https://cloud.google.com/kms/docs/algorithms
    EOF

  default = {
    purpose   = "ENCRYPT_DECRYPT"
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }
}

variable "rotation_period" {
  type        = "string"
  description = <<EOF
    To enable automation rotation of a key, set the rotation period.
    Rotation period must be in the form INTEGER[s], where unit 's' represents seconds
    Note that th required next_rotation_time will be set automatically to now + rotation_period.
    This field should be editable after resource creation.
  EOF
  default     = null
}

resource "google_kms_crypto_key" "crypto_key" {
  name            = var.name
  key_ring        = var.key_ring
  rotation_period = var.rotation_period
  purpose         = var.purpose_algorithm.purpose

  version_template {
    algorithm        = var.purpose_algorithm.algorithm
    protection_level = "SOFTWARE"
  }
  lifecycle {
    prevent_destroy = true
  }
}
