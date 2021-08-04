module "qreviews_prod_secret_bucket" {
  source     = "../src/resource/storage/bucket"
  name       = "${data.terraform_remote_state.cbh_kms_ref.outputs.qreviews_creds.ring_name}${local.secret_bucket_postfix}"
  project_id = module.cbh_secrets_project.project_id

  bucket_policy_data = [
    {
      role    = "roles/storage.admin"
      members = ["group:gcp-admins@cityblock.com"]
    }
  ]
}
