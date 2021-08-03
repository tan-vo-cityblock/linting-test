module "elation_events" {
  source  = "../src/resource/pubsub/topic"
  name    = "elationEvents"
  project = var.partner_project_production
  labels = {
    data = "phi"
  }
  topic_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com"
      ]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
      ]
    },
    {
      role = "roles/pubsub.publisher"
      members = [
        "serviceAccount:${google_service_account.elation_hook_svc_acct.email}"
      ]
    }
  ]
}

module "staging_elation_events" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingElationEvents"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com"
      ]
    }
  ]
}