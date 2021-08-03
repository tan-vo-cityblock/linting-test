output "name" {
  value = var.name
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
  description = "CloudSQL instance name for DB"
}

resource "google_sql_database" "db" {
  name      = var.name
  project   = var.project_id
  instance  = var.instance_name
  charset   = "UTF8"
  collation = "en_US.UTF8"
}
