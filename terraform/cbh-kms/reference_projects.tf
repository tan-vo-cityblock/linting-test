// Defined once here and used by multiple -kms.tf files to set decrypter IAM on project's default service accounts.

module "cityblock_orchestration_project_ref" {
  source     = "../src/data/project"
  project_id = "cityblock-orchestration"
}

module "cbh_db_mirror_prod_ref" {
  source = "../src/data/project"
  project_id = "cbh-db-mirror-prod"
}

module "cbh_db_mirror_staging_ref" {
  source = "../src/data/project"
  project_id = "cbh-db-mirror-staging"
}
