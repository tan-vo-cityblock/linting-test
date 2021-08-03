resource "google_folder" "personal_projects" {
  display_name = "Personal Projects - PHI"
  parent       = "organizations/250790368607"
}

resource "google_folder_iam_policy" "personal_projects_policy" {
  folder      = "${google_folder.personal_projects.name}"
  policy_data = "${data.google_iam_policy.personal_projects_folder_policy.policy_data}"
}

data "google_iam_policy" "personal_projects_folder_policy" {
  binding {
    role = "roles/resourcemanager.folderAdmin"

    members = [
      "group:gcp-admins@cityblock.com",
    ]
  }

  binding {
    role = "roles/resourcemanager.folderEditor"

    members = [
      "group:gcp-admins@cityblock.com",
    ]
  }

  binding {
    role = "roles/resourcemanager.folderViewer"

    members = [
      "domain:cityblock.com",
    ]
  }
}
