resource google_organization_iam_binding "org_viewers" {
  org_id = local.org_id
  role   = "roles/viewer"
  members = [
    "group:gcp-admins@cityblock.com",
    "user:lon.binder@cityblock.com"
  ]
}

resource google_organization_iam_binding "project_creators" {
  org_id = local.org_id
  role   = "roles/resourcemanager.projectCreator"
  members = [
    "group:gcp-admins@cityblock.com"
  ]
}
