import os
import base64
import logging

import requests
import json


ENG_TRIGGER_ID_TO_SERVICE = {
    "2b2a2683-e304-4fa9-b2dd-9e77bedeceb9": "qm-service",
    "b92dad8b-a677-4749-956e-ac6eff12271c": "member-service",
    "2b1ded47-6ba9-4e47-bfb4-43e5886e6ebe": "scio-jobs",
}
DATA_TRIGGER_ID_TO_SERVICE = {
    "894eefcc-4bb2-4115-9f3e-f69c0f1ba2e3": "dbt-prod-run"
}
VALID_ENG_STATUSES = ["SUCCESS", "FAILURE", "CANCELLED"]
VALID_DATA_STATUSES = ["QUEUED", "SUCCESS", "FAILURE", "CANCELLED", "TIMEOUT"]


def _set_emoji(status: str) -> str:
    if status == "SUCCESS":
        return ":white_check_mark:"
    elif status == "QUEUED":
        return ":clock1:"
    else:
        return ":x:"


def pubsub_to_slack(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """

    logging.info(f"This Function was triggered by messageId {context.event_id} published at {context.timestamp}")

    if 'data' in event:
        data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        status = data.get("status")
        trigger_id = data.get("buildTriggerId")
        webhook_url, build_name = None, None
        if status in VALID_ENG_STATUSES and ENG_TRIGGER_ID_TO_SERVICE.get(trigger_id):
            webhook_url = os.environ.get("ENG_SLACK_WEBHOOK")
            build_name = data["source"]["repoSource"].get("tagName") or ENG_TRIGGER_ID_TO_SERVICE.get(trigger_id)
        if status in VALID_DATA_STATUSES and DATA_TRIGGER_ID_TO_SERVICE.get(trigger_id):
            webhook_url = os.environ.get("DATA_SLACK_WEBHOOK")
            build_name = DATA_TRIGGER_ID_TO_SERVICE.get(trigger_id)
        if build_name and webhook_url:
            try:
                # project number in URL fails to load page, but project id succeeds, don't know why
                log_url = data.get("logUrl").replace("97093835752", "cityblock-data")
                status_emoji = _set_emoji(status)
                req_data = {
                    "text": f"Build for {build_name} was {status} {status_emoji}\n <{log_url}|See build logs here>"
                }
                response = requests.post(webhook_url, data=json.dumps(req_data), headers={"Content-Type": "application/json"})

                logging.info(f"Response: {response.text}")
                logging.info(f"Response code: {response.status_code}")
                logging.info(f"Data payload: {data}")
            except Exception as e:
                logging.error(f"Failed to process messageId {context.event_id} - here is data: [{event['data']}]")
                raise e
