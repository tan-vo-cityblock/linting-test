variable "project_id" {
  type        = string
  description = "Project in which to grant the role in"
}

variable "members" {
  type        = list(string)
  description = <<EOT
    A list of members to be granted a role, must be of the following forms:
    person/user:      user:email@cityblock.com
    group:            group:group@cityblock.com
    service account:  serviceAccount:account@project.iam.gserviceaccount.com
  EOT
}

variable "role" {
  type        = string
  description = <<EOT
  Predefined role for access to a specific service in project
  Of the form `roles/service.permission`

  See https://cloud.google.com/iam/docs/understanding-roles for comprehensive list of predefined roles
  EOT
}

resource "google_project_iam_member" "iam_member" {
  for_each = toset(var.members)

  project = var.project_id
  role    = var.role
  member  = each.key
}
