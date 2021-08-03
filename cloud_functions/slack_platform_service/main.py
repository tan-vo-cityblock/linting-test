import hmac
import hashlib
import json
import logging
import os
from threading import Thread
from typing import Union, Tuple, Optional

import requests
from flask import jsonify, Request, Response
from google.cloud import storage


def _verify_signature(request: Request) -> None:
    timestamp = request.headers.get('X-Slack-Request-Timestamp', '')
    signature = request.headers.get('X-Slack-Signature', '')

    req = str.encode(f'v0:{timestamp}:') + request.get_data()
    request_digest = hmac.new(
        str.encode(os.environ['SLACK_SECRET']),
        req, hashlib.sha256
    ).hexdigest()
    request_hash = f'v0={request_digest}'

    if not hmac.compare_digest(request_hash, signature):
        raise ValueError('Invalid request/credentials.')

def _handle_oncall(team: str, response_url: str) -> None:
    team = team.lower()
    if team not in ['product', 'platform', 'data']:
        logging.error(f'Incorrect area provided: [{team}]')
        invalid_team_response = {
            'response_type': 'ephemeral',
            'text': 'Text must be either `platform` or `product` or `data`',
        }
        requests.post(response_url, data=json.dumps(invalid_team_response), headers={"Content-Type": "application/json"})

    logging.info(f'Fetching on call information for [{team}]')
    primary_oncall, secondary_oncall = None, None
    if team in ['product', 'platform']:
        primary_oncall, secondary_oncall = _find_oncall_from_pd(team, response_url)
    if team == 'data':
        primary_oncall, secondary_oncall = _find_oncall_from_gcs(team, response_url)

    res_data = {
        'response_type': 'ephemeral',
        'text': f'{primary_oncall} is primary and {secondary_oncall} is secondary on call for {team.capitalize()}'
    }
    requests.post(response_url, data=json.dumps(res_data), headers={"Content-Type": "application/json"})

def _find_oncall_from_pd(team: str, response_url: str) -> Optional[Tuple[str, str]]:
    area_to_policy_id = {
        'platform': 'PT9MKGF',
        'product': 'P533C5O',
    }
    api_token = os.environ.get('PAGERDUTY_API_TOKEN')
    pd_headers = {
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Authorization': f'Token token={api_token}'
    }
    payload = {'escalation_policy_ids[]': [area_to_policy_id[team]]}
    response = requests.get("https://api.pagerduty.com/oncalls", headers=pd_headers, params=payload)
    response.raise_for_status()
    try:
        primary, secondary = None, None
        for oncall in response.json().get('oncalls'):
            level = oncall.get('escalation_level')
            oncall_person = oncall['user']['summary']
            if level == 1:
                primary = oncall_person
            if level == 2:
                secondary = oncall_person
        return primary, secondary
    except (TypeError, KeyError):
        logging.error(f'Unable to find oncall for {team}')
    no_oncall_response = {
        'response_type': 'ephemeral',
        'text': f'No on call found for {team}'
    }
    requests.post(response_url, data=json.dumps(no_oncall_response), headers={"Content-Type": "application/json"})

def _find_oncall_from_gcs(team: str, response_url: str) -> Optional[Tuple[str, str]]:
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(os.environ.get('DATA_CONFIG_BUCKET'))
        blob = bucket.blob(f'oncall/{team}.json')
        oncall_data = json.loads(blob.download_as_string())
        return oncall_data.get('primary'), oncall_data.get('secondary')
    except Exception as e:
        logging.error(f"Got error when retrieving information from GCS: {str(e)}")
        no_oncall_response = {
            'response_type': 'ephemeral',
            'text': f'Got an error when fetching {team} info, please reach out to Platform team'
        }
        requests.post(response_url, data=json.dumps(no_oncall_response), headers={"Content-Type": "application/json"})
        raise

def slack_platform_service(request: Request) -> Response:
    """Main function that takes in a request and verifies it via Slack secret as that is
    expected to be the only way to invoke this method. After verification it undergoes a
    given path based on the nature of the request

    Currently the following paths are supported:
        - /oncall to determine who is on call via Pagerduty API (https://developer.pagerduty.com/api-reference/)

    Args:
        request (flask.Request): Flask Request object, default for HTTP trigger Cloud Functions
    """
    if request.method != 'POST':
        return 'Only POST requests are accepted', 405

    _verify_signature(request)

    if request.form['command'] == '/oncall':
        response_url = request.form["response_url"]
        team = request.form['text']
        # background process to ensure quick response time as per Slack minimum 3 second response time rule
        thr = Thread(target=_handle_oncall, args=[team, response_url])
        thr.start()
        return jsonify({
            'response_type': 'ephemeral',
            'text': f'Fetching on-call information for {team.capitalize()}...'
        })
