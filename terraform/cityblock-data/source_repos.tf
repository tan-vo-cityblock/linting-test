resource "google_sourcerepo_repository" "mixer_mirror" {
  project = var.partner_project_production
  name = "github_cityblock_mixer"
}

resource "google_sourcerepo_repository" "commons_repo" {
  project = var.partner_project_production
  name = "github_cityblock_commons"
}
