// Defined once here and used by multiple -kms.tf files to set decrypter IAM on project's default service accounts.

module "cityblock_data_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-data"
}
