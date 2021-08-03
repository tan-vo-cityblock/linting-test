import datetime

from airflow.contrib.kubernetes import secret

PAYER_GCP_PROJECT = "cbh-healthyblue-data"
SLUG = "healthyblue"
PAYER_NAME = "Healthy Blue"
LAUNCH_DATE = datetime.datetime(2021, 7, 1)
EMAIL_TO = [
    "data-team@cityblock.com",
    "gcp-admins@cityblock.com",
    "actuary@cityblock.com",
    "partner-data-eng@cityblock.com",
]
service_account_secret = secret.Secret(
    deploy_type="volume",
    deploy_target="/var/secrets/google",
    secret="tf-svc-healthyblue-worker",
    key="key.json",
)
