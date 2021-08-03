module "gitlab-ee" {
  source        = "../src/custom/gitlab_omnibus"
  project_id    = module.git_project.project_id
  instance_name = "gitlab-ee"
  dns_project   = "cityblock-dns"
}
