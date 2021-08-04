module "elation_events_scio_sub" {
  source  = "../src/resource/pubsub/subscription"
  name    = "elationEventsScioSub"
  topic   = module.elation_events.name
  project = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 20
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com",
      ]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
}

module "healthgorilla_elation_events" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "healthGorillaElationEventsSub"
  topic                = module.elation_events.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 20
  subscription_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${module.prod_healthgorilla_worker_svc_acct.email}"
      ]
    }
  ]
  push_endpoint = google_cloudfunctions_function.healthgorilla_ccd_updates.https_trigger_url
  default_oidc_token_email = module.prod_healthgorilla_worker_svc_acct.email
}

module "dev_healthgorilla_elation_events" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devHealthGorillaElationEventsSub"
  topic                = module.staging_elation_events.name
  project              = var.partner_project_production
  ack_deadline_seconds = 20
  subscription_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
        "group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${module.dev_healthgorilla_worker_svc_acct.email}"
      ]
    }
  ]
  push_endpoint = google_cloudfunctions_function.dev_healthgorilla_ccd_updates.https_trigger_url
  default_oidc_token_email = module.dev_healthgorilla_worker_svc_acct.email
}
