module "commons_add_user_to_patient_channel" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsAddUserToPatientChannel"
  topic                = module.add_user_to_patient_channel.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 20
  labels = {
    datadog = "prod"
  }
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

module "staging_commons_add_user_to_patient_channel" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsAddUserToPatientChannel"
  topic                = module.staging_add_user_to_patient_channel.name
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

module "dev_commons_add_user_to_patient_channel" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsAddUserToPatientChannel"
  topic                = module.dev_add_user_to_patient_channel.name
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

module "commons_cache_ccd_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsCacheCCDEncounter"
  topic                = module.cache_ccd_encounter.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 600
  labels = {
    datadog = "prod"
  }
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

module "staging_commons_cache_ccd_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsCacheCCDEncounter"
  topic                = module.staging_cache_ccd_encounter.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "dev_commons_cache_ccd_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsCacheCCDEncounter"
  topic                = module.dev_cache_ccd_encounter.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "commons_cache_claim_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsCacheClaimEncounter"
  topic                = module.cache_claim_encounter.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 300
  labels = {
    datadog = "prod"
  }
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_commons_cache_claim_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsCacheClaimEncounter"
  topic                = module.staging_cache_claim_encounter.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "dev_commons_cache_claim_encounter" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsCacheClaimEncounter"
  topic                = module.dev_cache_claim_encounter.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "commons_cache_patient_eligibilities" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsCachePatientEligibilities"
  topic                = module.cache_patient_eligibilities.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 300
  labels = {
    datadog = "prod"
  }
  subscription_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_commons_cache_patient_eligibilities" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsCachePatientEligibilities"
  topic                = module.staging_cache_patient_eligibilities.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "dev_commons_cache_patient_eligibilities" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsCachePatientEligibilities"
  topic                = module.dev_cache_patient_eligibilities.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
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

module "commons_calendar_event" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsCalendarEvent"
  topic                = module.calendar_event.name
  project              = var.partner_project_production
  dead_letter_topic_id = module.dead_letter_prod.id
  ack_deadline_seconds = 20
  labels = {
    datadog = "prod"
  }
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

module "staging_commons_calendar_event" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsCalendarEvent"
  topic                = module.staging_calendar_event.name
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

module "dev_commons_calendar_event" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsCalendarEvent"
  topic                = module.dev_calendar_event.name
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

module "commons_clinical_information_update" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsClinicalInformationUpdate"
  topic                = module.clinical_information_update_messages.name
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

module "staging_commons_clinical_information_update" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsClinicalInformationUpdate"
  topic                = module.staging_clinical_information_update_messages.name
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

module "dev_commons_clinical_information_update" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsClinicalInformationUpdate"
  topic                = module.dev_clinical_information_update_messages.name
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

module "commons_financial_assistance_evaluation" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsFinancialAssistanceEvaluation"
  topic                = module.financial_assistance_evaluation.name
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

module "staging_commons_financial_assistance_evaluation" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsFinancialAssistanceEvaluation"
  topic                = module.staging_financial_assistance_evaluation.name
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

module "dev_commons_financial_assistance_evaluation" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsFinancialAssistanceEvaluation"
  topic                = module.dev_financial_assistance_evaluation.name
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

module "commons_hello_sign_process" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsHelloSignProcess"
  topic                = module.hello_sign_process.name
  project              = var.partner_project_production
  ack_deadline_seconds = 20
  labels = {
    datadog = "prod"
  }
  dead_letter_topic_id = module.dead_letter_prod.id
  subscription_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
      "group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_commons_hello_sign_process" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsHelloSignProcess"
  topic                = module.staging_hello_sign_process.name
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
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_commons_hello_sign_process" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsHelloSignProcess"
  topic                = module.dev_hello_sign_process.name
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
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "commons_hie_event_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsHieEventMessages"
  topic                = module.hie_event_messages.name
  project              = var.partner_project_production
  ack_deadline_seconds = 300
  labels = {
    datadog = "prod"
  }
  dead_letter_topic_id = module.dead_letter_prod.id
  subscription_policy_data = [
    {
      role = "roles/pubsub.admin"
      members = [
      "group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.subscriber"
      members = [
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_commons_hie_event_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsHieEventMessages"
  topic                = module.staging_hie_event_messages.name
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
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_commons_hie_event_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsHieEventMessages"
  topic                = module.dev_hie_event_messages.name
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
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "commons_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsMemberDemographics"
  topic                = module.member_demographics.name
  project              = var.partner_project_production
  ack_deadline_seconds = 600
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

module "staging_commons_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsMemberDemographics"
  topic                = module.staging_member_demographics.name
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

module "dev_commons_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsMemberDemographics"
  topic                = module.dev_member_demographics.name
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

module "cureatr_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "cureatrMemberDemographics"
  topic                = module.member_demographics.name
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
        "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}"
      ]
    }
  ]
  push_endpoint            = google_cloudfunctions_function.cureatr_member_updates.https_trigger_url
  default_oidc_token_email = module.prod_cureatr_worker_svc_acct.email
}

module "dev_cureatr_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCureatrMemberDemographics"
  topic                = module.staging_member_demographics.name
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
        "serviceAccount:${module.dev_cureatr_worker_svc_acct.email}"
      ]
    }
  ]
  push_endpoint            = google_cloudfunctions_function.dev_cureatr_member_updates.https_trigger_url
  default_oidc_token_email = module.dev_cureatr_worker_svc_acct.email
}

module "healthgorilla_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "healthGorillaMemberDemographics"
  topic                = module.member_demographics.name
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
  push_endpoint            = google_cloudfunctions_function.healthgorilla_member_updates.https_trigger_url
  default_oidc_token_email = module.prod_healthgorilla_worker_svc_acct.email
}

module "dev_healthgorilla_member_demographics" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devHealthGorillaMemberDemographics"
  topic                = module.staging_member_demographics.name
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
  push_endpoint            = google_cloudfunctions_function.dev_healthgorilla_member_updates.https_trigger_url
  default_oidc_token_email = module.dev_healthgorilla_worker_svc_acct.email
}


module "commons_computed_fields" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsComputedFields"
  topic                = module.new_computed_field_results.name
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

module "staging_commons_computed_fields" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsComputedFields"
  topic                = module.staging_new_computed_field_results.name
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

module "dev_commons_computed_fields" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsComputedFields"
  topic                = module.dev_new_computed_field_results.name
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

module "commons_scheduling_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsSchedulingMessages"
  topic                = module.scheduling_messages.name
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

module "staging_commons_scheduling_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsSchedulingMessages"
  topic                = module.staging_scheduling_messages.name
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

module "dev_commons_scheduling_messages" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsSchedulingMessages"
  topic                = module.dev_scheduling_messages.name
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

module "commons_save_map" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsSaveMap"
  topic                = module.save_map.name
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

module "staging_commons_save_map" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsSaveMap"
  topic                = module.staging_save_map.name
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

module "dev_commons_save_map" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsSaveMap"
  topic                = module.dev_save_map.name
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

module "prod_commons_run_dataflow_template" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "runCommonsDataflowTemplate"
  topic                = module.run_dataflow_template.name
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
  push_endpoint            = google_cloudfunctions_function.dataflow_templates_runner_prod.https_trigger_url
  default_oidc_token_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
}

module "staging_commons_run_dataflow_template" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingRunCommonsDataflowTemplate"
  topic                = module.staging_run_dataflow_template.name
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
  push_endpoint            = google_cloudfunctions_function.dataflow_templates_runner_staging.https_trigger_url
  default_oidc_token_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
}

module "dev_commons_run_dataflow_template" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devRunCommonsDataflowTemplate"
  topic                = module.dev_run_dataflow_template.name
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
  push_endpoint            = google_cloudfunctions_function.dataflow_templates_runner_dev.https_trigger_url
  default_oidc_token_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
}

module "prod_commons_meds_file_scripting" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "runCommonsMedsFileScripting"
  topic                = module.run_cureatr_meds_file_scripting.name
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
        "serviceAccount:${module.prod_cureatr_worker_svc_acct.email}"
      ]
    }
  ]
}

module "dev_commons_patient_tends" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "devCommonsPatientTends"
  topic                = module.dev_patient_tends.name
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
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "staging_commons_patient_tends" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "stagingCommonsPatientTends"
  topic                = module.staging_patient_tends.name
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

module "commons_patient_tends" {
  source               = "../src/resource/pubsub/subscription"
  name                 = "commonsPatientTends"
  topic                = module.patient_tends.name
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
