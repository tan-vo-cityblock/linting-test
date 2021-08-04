module "dead_letter_prod" {
  source  = "../src/resource/pubsub/topic"
  name    = "deadLetter"
  project = var.partner_project_production
  labels = {
    data = "phi"
  }
  topic_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com",
      ]
    },
    {
      role = "roles/pubsub.publisher"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_pubsub_service_account_email}",
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
}
