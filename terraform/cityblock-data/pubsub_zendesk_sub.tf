module "commons_zendesk_ticket_sub" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsZendeskTicket"
  topic                = module.zendesk_ticket_topic.name
  project              = var.partner_project_production
  ack_deadline_seconds = 20
  labels = {
    datadog = "prod"
  }
  dead_letter_topic_id = module.dead_letter_prod.id
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_commons_zendesk_ticket_sub" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsZendeskTicket"
  topic                = module.staging_zendesk_ticket_topic.name
  project              = var.partner_project_production
  ack_deadline_seconds = 20
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_commons_zendesk_ticket_sub" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsZendeskTicket"
  topic                = module.dev_zendesk_ticket_topic.name
  project              = var.partner_project_production
  ack_deadline_seconds = 20
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}
