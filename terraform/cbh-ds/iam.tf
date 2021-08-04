module "project_editors" {
  source     = "../src/custom/project_iam_access"
  project_id = module.data_science_project.project_id
  role       = "roles/owner"
  members = [
    "group:data-science@cityblock.com"
  ]
}
