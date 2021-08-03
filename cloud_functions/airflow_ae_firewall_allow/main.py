import logging
import os
from typing import Dict, List, Literal, Optional, Union

import google.auth
from flask import Request, Response, jsonify
from google.cloud.container_v1 import ClusterManagerClient
from googleapiclient.discovery import build
from kubernetes import client, config

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
CREDENTIALS, _ = google.auth.default(scopes=SCOPES)
COMPOSER_GKE_PROJECT = os.environ["COMPOSER_GKE_PROJECT"]
COMPOSER_GKE_ZONE = os.environ["COMPOSER_GKE_ZONE"]
COMPOSER_GKE_NAMES = {
    'prod': os.environ["COMPOSER_GKE_NAME_PROD"],
    'test': os.environ["COMPOSER_GKE_NAME_TEST"],
}


Env = Union[Literal["prod"], Literal["test"]]


def firewall_service():
    return (
        build(
            'appengine',
            'v1',
            credentials=CREDENTIALS,
            cache_discovery=False
        ).apps().firewall().ingressRules()
    )


def fetch_airflow_ips(env: Env) -> List[str]:
    cluster_id = COMPOSER_GKE_NAMES[env]
    if cluster_id is None:
        raise ValueError
    logging.info(
        f'Attempting to get IP Addresses for GKE cluster: {cluster_id}')
    cluster_manager_client = ClusterManagerClient(credentials=CREDENTIALS)
    cluster = cluster_manager_client.get_cluster(
        COMPOSER_GKE_PROJECT, COMPOSER_GKE_ZONE, cluster_id)
    configuration = client.Configuration()
    configuration.host = "https://" + cluster.endpoint + ":443"
    configuration.verify_ssl = False
    configuration.api_key = {"authorization": "Bearer " + CREDENTIALS.token}
    client.Configuration.set_default(configuration)

    airflow_ips = []
    v1 = client.CoreV1Api()
    nodes = v1.list_node()
    for node in nodes.items:
        for address in node.status.addresses:
            if address.type == "ExternalIP":
                airflow_ips.append(address.address)

    logging.info(
        f'Successfully retrieved External IP Addresses, found {len(airflow_ips)} of them')
    return airflow_ips


def get_airflow_rules(appId: str, env: Env) -> List[Dict[str, Union[str, int]]]:
    logging.info(f'Attempting to get firewall rules for app: {appId}')
    all_rules = firewall_service().list(appsId=appId).execute()

    airflow_rules = []
    for rule in all_rules['ingressRules']:
        desc = rule.get('description', "")
        if "airflow" in desc and env in desc:
            airflow_rules.append(rule)

    logging.info(
        f'Successfully retrieved all firewall rules, found {len(airflow_rules)} of them')
    return airflow_rules


def patch_rules_if_needed(ips: List[str], rules: List[Dict[str, Union[str, int]]], appId: str, env: Env) -> bool:
    assert len(ips) == len(
        rules), "Must have the same amount of rules as IP addresses"
    new_ips = []
    for ip_address in ips:
        if ip_address in map(lambda rule: rule['sourceRange'], rules):
            logging.info(f'Address already found in rules: {ip_address}')
            matching_rule = next(
                rule for rule in rules if rule['sourceRange'] == ip_address)
            rules.remove(matching_rule)
        else:
            new_ips.append(ip_address)

    if len(new_ips):
        for ip_address, old_rule in zip(new_ips, rules):
            logging.info(
                f'this IP is considered stale [{old_rule["sourceRange"]}], will remove from rules')
            old_rule['sourceRange'] = ip_address
            old_rule['description'] = f"airflow {env}"
            firewall_service().patch(
                appsId=appId,
                ingressRulesId=old_rule['priority'],
                body=old_rule,
                updateMask='sourceRange'
            ).execute()
            logging.info(f'updated the following rule: {old_rule}')
        return True
    else:
        logging.info('no rules to be updated! Carry on')
        return False


def check_and_maybe_update_rules(request: Request) -> Response:
    """Given the configuration details in the request body we retrieve the IP addresses of the Airflow
    machines and confirm if they have a firewall approval rule with an App Engine instance.
    If not, we update those rules accordingly.

    If the Airflow machines are already allowed on the App Engine instance, nothing occurs.
    If there is a mismatch, the rules are updated to ensure the Airflow machines are allowed.

    Key assumption is the App Engine instance already contains rules for the Airflow instance with the
    term 'airflow' in the description of it.

    :param request: HTTP Request made to an endpoint which is then routed to this method
    :return: Flask Response object containing status of what was done (no updates or updates done)
    """
    if request.json.get('project_id') or request.json.get('zone'):
        logging.warn(
            f"warning: this function does not recognize project_id or zone in trigger event json anymore")
    app_engine = request.json['ae_instance']
    env: Env = 'prod' if 'prod' in app_engine else 'test'

    airflow_ips = fetch_airflow_ips(env)
    airflow_rules = get_airflow_rules(app_engine, env)
    update_made = patch_rules_if_needed(
        airflow_ips, airflow_rules, app_engine, env)
    message = "UPDATED RULE(S)" if update_made else "NO UPDATES MADE"
    return jsonify(f"{message} for {app_engine}")
