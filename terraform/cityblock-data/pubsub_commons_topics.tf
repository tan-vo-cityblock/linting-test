module "add_user_to_patient_channel" {
  source  = "../src/resource/pubsub/topic"
  name    = "addUserToPatientChannel"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_add_user_to_patient_channel" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingAddUserToPatientChannel"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_add_user_to_patient_channel" {
  source  = "../src/resource/pubsub/topic"
  name    = "devAddUserToPatientChannel"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "cache_ccd_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "cacheCCDEncounter"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_cache_ccd_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingCacheCCDEncounter"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}",
      ]
    }
  ]
}

module "dev_cache_ccd_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "devCacheCCDEncounter"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}",
      ]
    }
  ]
}


module "cache_claim_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "cacheClaimEncounter"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_cache_claim_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingCacheClaimEncounter"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_cache_claim_encounter" {
  source  = "../src/resource/pubsub/topic"
  name    = "devCacheClaimEncounter"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}


module "cache_patient_eligibilities" {
  source  = "../src/resource/pubsub/topic"
  name    = "cachePatientEligibilities"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_cache_patient_eligibilities" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingCachePatientEligibilities"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_cache_patient_eligibilities" {
  source  = "../src/resource/pubsub/topic"
  name    = "devCachePatientEligibilities"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "calendar_event" {
  source  = "../src/resource/pubsub/topic"
  name    = "calendarEvent"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_calendar_event" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingCalendarEvent"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_calendar_event" {
  source  = "../src/resource/pubsub/topic"
  name    = "devCalendarEvent"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "financial_assistance_evaluation" {
  source  = "../src/resource/pubsub/topic"
  name    = "financialAssistanceEvaluation"
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
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_financial_assistance_evaluation" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingFinancialAssistanceEvaluation"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_financial_assistance_evaluation" {
  source  = "../src/resource/pubsub/topic"
  name    = "devFinancialAssistanceEvaluation"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}


module "hello_sign_process" {
  source  = "../src/resource/pubsub/topic"
  name    = "helloSignProcess"
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
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_hello_sign_process" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingHelloSignProcess"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_hello_sign_process" {
  source  = "../src/resource/pubsub/topic"
  name    = "devHelloSignProcess"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "new_computed_field_results" {
  source  = "../src/resource/pubsub/topic"
  name    = "newComputedFieldResults"
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
        "group:data-team@cityblock.com",
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_new_computed_field_results" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingNewComputedFieldResults"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "group:data-team@cityblock.com",
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_new_computed_field_results" {
  source  = "../src/resource/pubsub/topic"
  name    = "devNewComputedFieldResults"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "group:data-team@cityblock.com",
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "save_map" {
  source  = "../src/resource/pubsub/topic"
  name    = "saveMap"
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
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}

module "staging_save_map" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingSaveMap"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_save_map" {
  source  = "../src/resource/pubsub/topic"
  name    = "devSaveMap"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "run_dataflow_template" {
  source  = "../src/resource/pubsub/topic"
  name    = "runDataflowTemplate"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_run_dataflow_template" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingRunDataflowTemplate"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_run_dataflow_template" {
  source  = "../src/resource/pubsub/topic"
  name    = "devRunDataflowTemplate"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "run_cureatr_meds_file_scripting" {
  source  = "../src/resource/pubsub/topic"
  name    = "runCureatrMedsFileScripting"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_prod_account}",
      ]
    }
  ]
}

module "staging_run_cureatr_meds_file_scripting" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingRunCureatrMedsFileScripting"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}",
      ]
    }
  ]
}

module "dev_run_cureatr_meds_file_scripting" {
  source  = "../src/resource/pubsub/topic"
  name    = "devRunCureatrMedsFileScripting"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "dev_patient_tends" {
  source  = "../src/resource/pubsub/topic"
  name    = "devPatientTends"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "staging_patient_tends" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingPatientTends"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "patient_tends" {
  source  = "../src/resource/pubsub/topic"
  name    = "patientTends"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_prod_account}"
      ]
    }
  ]
}
