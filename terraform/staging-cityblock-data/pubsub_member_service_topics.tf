module "memberAttribution" {
  source  = "../src/resource/pubsub/topic"
  name    = "memberAttribution"
  project = var.project_staging
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/pubsub.publisher"
      members = ["serviceAccount:97093835752-compute@developer.gserviceaccount.com"]
    }
  ]
}

module "memberDemographics_pub_sub" {
  source  = "../src/resource/pubsub/topic"
  name    = "memberDemographics"
  project = var.project_staging
  topic_policy_data = [
    {
      role    = "roles/pubsub.admin"
      members = ["group:gcp-admins@cityblock.com"]
    },
    {
      role = "roles/pubsub.publisher"
      members = [
        "serviceAccount:97093835752-compute@developer.gserviceaccount.com",
        "serviceAccount:${module.cbh_member_service_staging_ref.project_id}@appspot.gserviceaccount.com"
      ]
    }
  ]
}
