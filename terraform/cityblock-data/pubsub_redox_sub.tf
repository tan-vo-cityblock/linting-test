module "health_events_raw_hie_sub" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "healthEventsRawHie"
  topic                = module.raw_hie_event_messages.name
  project              = var.partner_project_production
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "group:eng-all@cityblock.com",
      ]
    }
  ]
  push_endpoint = "https://health-events-service-prod-bz54ntlpoq-ue.a.run.app/hie-messages"
}

module "staging_health_events_raw_hie_sub" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingHealthEventsRawHie"
  topic                = module.staging_raw_hie_event_messages.name
  project              = var.partner_project_production
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "group:eng-all@cityblock.com",
      ]
    }
  ]
  push_endpoint = "https://health-events-service-staging-7a4htmb5sq-uk.a.run.app/hie-messages"
}
