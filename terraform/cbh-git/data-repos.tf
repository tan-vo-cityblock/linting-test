// Contains all repos/projects for the data team

resource "google_sourcerepo_repository" "picasso_mirror" {
  project = module.git_project.project_id
  name    = "picasso_mirror"
}
