resource "google_cloudfunctions_function" "elationHookEventReceiver" {
  name                  = "elationHookEventReceiver"
  project               = var.partner_project_production
  description           = "Webhook function into elation that parses post requests for elation events"
  available_memory_mb   = 128
  timeout               = 60
  entry_point           = "elationHookEventReceiver"
  runtime               = "nodejs8"
  trigger_http          = true
  service_account_email = google_service_account.elation_hook_svc_acct.email
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/elation_hook"
  }
  region = "us-central1"

  labels = {
    deployment-tool = "cli-gcloud"
  }
}

resource "google_cloudfunctions_function" "airflow_ae_firewall_allow" {
  project               = var.partner_project_production
  name                  = "airflow_ae_firewall_allow"
  runtime               = "python37"
  description           = "Update Airflow IP approval status with App Engine instance"
  available_memory_mb   = 128
  timeout               = 60
  entry_point           = "check_and_maybe_update_rules"
  trigger_http          = true
  service_account_email = module.airflow_app_engine_svc_acct.email
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/airflow_ae_firewall_allow"
  }
  region = "us-east4"
}


resource "google_cloudfunctions_function" "carefirst-daily-dag-trigger" {
  project               = var.partner_project_production
  name                  = "carefirst-daily-dag-trigger"
  runtime               = "python37"
  description           = "Carefirst member file drops are loosely daily and consistently unpredictable. This cloud function replaces cloud composer cron-based scheduling with GCS file activity event->cloud function triggering."
  available_memory_mb   = 128
  timeout               = 60
  entry_point           = "trigger_daily_carefirst_member_ingestion_dag"
  service_account_email = var.load_gcs_data_cf_svc_acct
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = module.sftp_drop_bucket.name
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/carefirst-daily-dag-trigger"
  }
  region = "us-east4"
}


# N.B. resource was imported so no source repository attached
resource "google_cloudfunctions_function" "slack_platform_service" {
  project               = var.partner_project_production
  name                  = "slack_platform_service"
  runtime               = "python37"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "slack_platform_service"
  trigger_http          = true
  service_account_email = module.slack_platform_bot_svc_acct.email
  environment_variables = {
    "DATA_CONFIG_BUCKET"  = "cbh-analytics-configs"
    "PAGERDUTY_API_TOKEN" = var.pagerduty_api_key
    "SLACK_SECRET"        = var.platform_bot_slack_secret
  }
  region = "us-east4"
  labels = {
    deployment-tool = "cli-gcloud"
  }
}

resource "google_cloudfunctions_function" "pubsub_bq_saver_member_demographics" {
  project               = var.partner_project_production
  name                  = "saveMemberDemographicsToBigQuery"
  description           = "Saving Pub/Sub messages from the member demographics topic to BigQuery."
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "saveMessageToBigQuery"
  service_account_email = module.pubsub_bq_saver_cf_svc_acct.email
  environment_variables = {
    BIGQUERY_PROJECT_ID = var.partner_project_production
    BIGQUERY_DATASET    = module.pubsub_messages_dataset.dataset_id
    BIGQUERY_TABLE      = module.member_demographics_table.table_id
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = module.member_demographics.id
  }

  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/pubsub-save-to-bq"
  }
}

resource "google_cloudfunctions_function" "pubsub_bq_saver_deadletter" {
  project               = var.partner_project_production
  name                  = "saveDeadLetterToBigQuery"
  description           = "Saving Pub/Sub messages from the dead letter topic to BigQuery for analysis"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "saveMessageToBigQuery"
  service_account_email = module.pubsub_bq_saver_cf_svc_acct.email
  environment_variables = {
    BIGQUERY_PROJECT_ID = var.partner_project_production
    BIGQUERY_DATASET    = module.pubsub_messages_dataset.dataset_id
    BIGQUERY_TABLE      = module.dead_letter_table.table_id
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = module.dead_letter_prod.id
  }

  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/pubsub-save-to-bq"
  }
}

resource "google_cloudfunctions_function" "dataflow_templates_runner_prod" {
  project               = var.partner_project_production
  name                  = "dataflowTemplatesRunner"
  description           = "Runs a Dataflow Flex Template"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "runDataflowTemplate"
  service_account_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
  trigger_http          = true
  environment_variables = {
    ENV = "production"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/dataflow-templates"
  }
}

resource "google_cloudfunctions_function" "dataflow_templates_runner_staging" {
  project               = var.partner_project_production
  name                  = "stagingDataflowTemplatesRunner"
  description           = "Runs a Dataflow Flex Template"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "runDataflowTemplate"
  service_account_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
  environment_variables = {
    ENV = "staging"
  }
  trigger_http = true
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/dataflow-templates"
  }
}

resource "google_cloudfunctions_function" "dataflow_templates_runner_dev" {
  project               = var.partner_project_production
  name                  = "devDataflowTemplatesRunner"
  description           = "Runs a Dataflow Flex Template"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "runDataflowTemplate"
  service_account_email = module.cityblock_data_project_ref.default_dataflow_job_runner_email
  environment_variables = {
    ENV = "development"
  }
  trigger_http = true
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/dataflow-templates"
  }
}

resource "google_cloudfunctions_function" "cureatr_member_updates" {
  project               = var.partner_project_production
  name                  = "cureatrMemberUpdates"
  description           = "Runs Cureatr member updates and create"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "updateOrCreateMember"
  service_account_email = module.prod_cureatr_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "production"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/cureatr-member-updates"
  }
}

resource "google_cloudfunctions_function" "dev_cureatr_member_updates" {
  project               = var.partner_project_production
  name                  = "devCureatrMemberUpdates"
  description           = "Runs Cureatr member updates and create"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "updateOrCreateMember"
  service_account_email = module.dev_cureatr_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "test"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/cureatr-member-updates"
  }
}

resource "google_cloudfunctions_function" "cureatr_batch_meds_refreshes" {
  project               = var.partner_project_production
  name                  = "cureatrBatchMedsRefreshes"
  description           = "Runs scripting of list of members to refresh meds list in Cureatr"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "writeMedsRefreshFile"
  service_account_email = module.prod_cureatr_worker_svc_acct.email
  trigger_http          = true
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/cureatr-batch-pharm-refreshes"
  }
}

resource "google_cloudfunctions_function" "cureatr_sftp_upload" {
  project               = var.partner_project_production
  name                  = "cureatrSftpSync"
  description           = "Runs uploading of files from GCS to Cureatr SFTP"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "uploadToCureatr"
  service_account_email = module.prod_cureatr_worker_svc_acct.email
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = module.temp_cureatr_rx_batch_bucket.name
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/upload-to-cureatr-sftp"
  }
}

resource "google_cloudfunctions_function" "healthgorilla_member_updates" {
  project               = var.partner_project_production
  name                  = "healthGorillaMemberUpdates"
  description           = "Runs HealthGorilla member updates and create"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "updateOrCreateMember"
  service_account_email = module.prod_healthgorilla_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "production"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/healthgorilla-member-updates"
  }
}

resource "google_cloudfunctions_function" "dev_healthgorilla_member_updates" {
  project               = var.partner_project_production
  name                  = "devHealthGorillaMemberUpdates"
  description           = "Runs HealthGorilla member updates and create"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "updateOrCreateMember"
  service_account_email = module.dev_healthgorilla_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "test"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/healthgorilla-member-updates"
  }
}

resource "google_cloudfunctions_function" "healthgorilla_ccd_updates" {
  project               = var.partner_project_production
  name                  = "healthGorillaCcdUpdates"
  description           = "Uploads CCDAs to HealthGorilla"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "uploadCCDA"
  service_account_email = module.prod_healthgorilla_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "production"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/healthgorilla-ccd-updates"
  }
}

resource "google_cloudfunctions_function" "dev_healthgorilla_ccd_updates" {
  project               = var.partner_project_production
  name                  = "devHealthGorillaCcdUpdates"
  description           = "Uploads CCDAs to HealthGorilla"
  runtime               = "nodejs10"
  region                = "us-central1"
  available_memory_mb   = 256
  timeout               = 60
  entry_point           = "uploadCCDA"
  service_account_email = module.dev_healthgorilla_worker_svc_acct.email
  trigger_http          = true
  environment_variables = {
    ENV = "test"
  }
  source_repository {
    url = "https://source.developers.google.com/${google_sourcerepo_repository.mixer_mirror.id}/moveable-aliases/master/paths/cloud_functions/healthgorilla-ccd-updates"
  }
}



module "sftp_drop_trigger" {
  source        = "./sftp_drop_trigger"
  sourcerepo_id = google_sourcerepo_repository.mixer_mirror.id
}
