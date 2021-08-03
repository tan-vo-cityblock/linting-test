variable "secret_name" {
  type        = "string"
  description = "secret name, will namespace to all data stored in the data block"
}

variable "secret_data" {
  type        = map(string)
  description = "key - value map of type string. Keys will be secret data name and value will be the secret"
}


resource "kubernetes_secret" "secret" {
  metadata {
    name = var.secret_name
  }
  data = var.secret_data
}
