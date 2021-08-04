module "clinical_information_update_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "clinicalInformationUpdateMessages"
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
      ]
    }
  ]
}

module "staging_clinical_information_update_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingClinicalInformationUpdateMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_clinical_information_update_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "devClinicalInformationUpdateMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}


module "hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "hieEventMessages"
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    }
  ]
}

module "staging_hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingHieEventMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "devHieEventMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}

module "raw_hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "rawHieEventMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
      ]
    }
  ]
}

module "staging_raw_hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingRawHieEventMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
      ]
    }
  ]
}

module "dev_raw_hie_event_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "devRawHieEventMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
      ]
    }
  ]
}

module "scheduling_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "schedulingMessages"
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
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}"
      ]
    },
    {
      role = "roles/pubsub.publisher",
      members = [
        "serviceAccount:97093835752-compute@developer.gserviceaccount.com"
      ]
    }
  ]
}

module "staging_scheduling_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingSchedulingMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${var.commons_staging_account}"
      ]
    },
    {
      role = "roles/pubsub.publisher",
      members = [
        "serviceAccount:97093835752-compute@developer.gserviceaccount.com"
      ]
    }
  ]
}

module "dev_scheduling_messages" {
  source  = "../src/resource/pubsub/topic"
  name    = "devSchedulingMessages"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role = "roles/pubsub.editor"
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}
