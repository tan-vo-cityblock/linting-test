locals {
  log_settings_names = ["log_duration", "log_checkpoints", "log_statement_stats"]
  turn_on_logs       = var.enable_logs ? "on" : "off"
}

variable "project_id" {
  type        = "string"
  description = "Project where sql instance will be created"
}

variable "name" {
  type        = "string"
  description = "name of sql instance"
}

variable "machine_type" {
  type        = "string"
  description = "Shared core machine type options: db-f1-micro, db-g1-small"
  default     = "db-g1-small"
}

variable "db_version" {
  type        = "string"
  description = "The MySQL or PostgreSQL version to use"
  default     = "POSTGRES_11"
}


variable "region" {
  type        = string
  description = "The region the instance will sit in. Choose from here: https://cloud.google.com/sql/docs/postgres/locations"
  default     = "us-east1"
}

variable "enable_logs" {
  type        = bool
  description = "Enable a set of logs to be reported by the instance and it's database(s). Note this should be switched to off when not needed to save resource use."
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Map of labels to apply to the Cloud SQL Instance"
}

resource "google_sql_database_instance" "instance" {
  project          = var.project_id
  name             = var.name
  database_version = var.db_version
  region           = var.region

  settings {
    tier              = var.machine_type
    activation_policy = "ALWAYS"
    availability_type = "REGIONAL"
    backup_configuration {
      enabled    = true
      start_time = "23:59"
    }

    disk_autoresize = true
    disk_size       = "12"
    disk_type       = "PD_SSD"
    user_labels     = var.labels

    maintenance_window {
      day          = 1
      hour         = 23
      update_track = "stable"
    }

    dynamic "database_flags" {
      for_each = local.log_settings_names
      content {
        name  = database_flags.value
        value = local.turn_on_logs
      }
    }
  }

  lifecycle {
    ignore_changes = ["settings[0].disk_size"]
  }
}
