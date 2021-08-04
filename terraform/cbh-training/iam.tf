resource "google_project_iam_member" "it_owner" {
  project = module.training_project.project_id
  member  = "group:it-team@cityblock.com"
  role    = "roles/owner"
}
