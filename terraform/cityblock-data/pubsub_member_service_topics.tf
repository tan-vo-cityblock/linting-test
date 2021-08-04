module "member_demographics" {
  source  = "../src/resource/pubsub/topic"
  name    = "memberDemographics"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role    = "roles/pubsub.editor",
      members = [
        "serviceAccount:${var.commons_prod_account}",
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${module.cbh_member_service_prod_ref.project_id}@appspot.gserviceaccount.com"
      ]
    }
  ]
}

module "staging_member_demographics" {
  source  = "../src/resource/pubsub/topic"
  name    = "stagingMemberDemographics"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role    = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${var.commons_staging_account}"
      ]
    }
  ]
}

module "dev_member_demographics" {
  source  = "../src/resource/pubsub/topic"
  name    = "devMemberDemographics"
  project = var.partner_project_production
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:eng-all@cityblock.com"]
    },
    {
      role    = "roles/pubsub.editor",
      members = [
        "serviceAccount:${module.cityblock_data_project_ref.default_dataflow_job_runner_email}",
        "serviceAccount:${module.commons_dev_svc_acct.email}"
      ]
    }
  ]
}
