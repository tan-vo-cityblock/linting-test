output "name" {
  value = google_sql_user.postgres_user.name
}

variable "name" {
  type        = "string"
  description = "Name of your database"
}

variable "project_id" {
  type        = "string"
  description = "Project where sql database will be created"
}

variable "instance_name" {
  type        = "string"
  description = "CloudSQL instance name user will have access to."
}

variable "password" {
  type        = "string"
  description = "User password"
}

// DO not set optional "host" field for PostgreSQL instances.
resource "google_sql_user" "postgres_user" {
  name     = var.name
  instance = var.instance_name
  project  = var.project_id
  password = var.password
}
