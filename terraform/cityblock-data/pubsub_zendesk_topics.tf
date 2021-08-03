module "zendesk_ticket_topic" {
  source  = "../src/resource/pubsub/topic"
  name    = "zendeskTicket"
  project = var.partner_project_production
  labels = {
    data = "phi"
  }
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.prod_zendesk_worker_svc_acct.email}"
      ]
    }
  ]
}

module "staging_zendesk_ticket_topic" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingZendeskTicket"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}"
      ]
    }
  ]
}

module "dev_zendesk_ticket_topic" {
  source  = "../src/resource/pubsub/topic"
  name    = "devZendeskTicket"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.staging_zendesk_worker_svc_acct.email}"
      ]
    }
  ]
}
