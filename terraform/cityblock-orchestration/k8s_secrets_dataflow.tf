resource "google_service_account_key" "dataflow_runner_key" {
  service_account_id = "projects/cityblock-data/serviceAccounts/dataflow-job-runner@cityblock-data.iam.gserviceaccount.com"
}

module "k8s_dataflow_runner_secret" {
  source = "../src/resource/kubernetes"
  secret_name = "tf-dataflow-runner-cityblock-data"
  secret_data = {
    "key.json": base64decode(google_service_account_key.dataflow_runner_key.private_key)
  }
}
