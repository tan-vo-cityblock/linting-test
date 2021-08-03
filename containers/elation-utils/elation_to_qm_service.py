import os
import logging
import argparse
import json
from timeit import default_timer
import asyncio

import pytz
import requests
import aiohttp
from google.cloud import bigquery

_QM_SVC_ENV_BASE_URLS = {
    "prod": "https://quality-measure-dot-cbh-services-prod.appspot.com/1.0",
    "staging": "https://quality-measure-dot-cbh-services-staging.appspot.com/1.0",
    "dev": "http://127.0.0.1:8080/1.0"
}


def _generate_member_qms_payload(bq_results, elation_tags_mapped_qms):
    qms_payload = {}
    for row in bq_results:
        row_dict = dict(list(row.items()))
        elation_tag = row_dict['codeType'].lower() + row_dict['code']

        if elation_tag not in elation_tags_mapped_qms:
            continue

        pt_tz = pytz.timezone('US/Pacific')  # timezone for elation db
        utc_tz = pytz.UTC
        set_on = row_dict["setOn"]
        set_on_pt_tz = pt_tz.localize(set_on)  # determines PST vs PDT automatically based on naive set_on datetime
        set_on_utc_tz = set_on_pt_tz.astimezone(utc_tz)  # conver to UTC for QM Service where all time is in UTC
        set_on_iso_format = set_on_utc_tz.isoformat().replace("+00:00", "Z")

        member_id = row['memberId']
        qm_row = {
            'userId': row['userId'],
            'setOn': set_on_iso_format,
            'id': elation_tags_mapped_qms[elation_tag]['measureId'],
            'status': elation_tags_mapped_qms[elation_tag]['statusName'],
            'reason': row['reason']
        }
        if member_id in qms_payload:
            qms_payload[member_id].append(qm_row)
        else:
            qms_payload[member_id] = [qm_row]

    deduped_member_qms_payload = {}
    measures_count = 0
    for memberId, payload in qms_payload.items():
        deduped_payload = [i for n, i in enumerate(payload) if i not in payload[n + 1:]]
        deduped_member_qms_payload[memberId] = deduped_payload
        measures_count = measures_count + len(deduped_payload)

    logging.info(f'{measures_count} measures for {len(deduped_member_qms_payload)} members will be posted.')

    return deduped_member_qms_payload


# POST function
async def _post(base_url, member_id, payload, session):
    member_qm_url = f'{base_url}/member/{member_id}/measures'

    async with session.post(member_qm_url, json=payload) as response:
        if response.status != 200:
            error = await response.content.decode('utf-8') # error is json and so content must be decoded.

            logging.info(f"\nFAILURE: {member_qm_url}")
            logging.info(f"\nQM_SERVICE ERROR: {error}\n")

            response.raise_for_status()  # stops execution of for loop and script if qm service responds with an error
        return await response.text()


async def _async_post_to_qm_service(qm_svc_base_url, member_qms_payload, qm_api_key):
    tasks = []

    # limit amount of simultaneously opened connections, default aiohttp is 100
    # ref https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size

    # per gcp issue thread comment, db-g1-small default max connections is 50.
    # https://issuetracker.google.com/issues/37271935#comment49
    # However, I ran into issues with TCPConnector set above 15.
    # Ability to increase max connections on cloudsql instance currently in beta:
    # https://cloud.google.com/sql/docs/postgres/flags#postgres-m
    conn = aiohttp.TCPConnector(limit=15)

    # Create client session that will ensure we don't open new connection per each request.
    # Fetch all responses within one Client session, keep connection alive for all requests.
    async with aiohttp.ClientSession(connector=conn, headers={'apiKey': qm_api_key}) as session:
        for member_id, payload in member_qms_payload.items():
            deduped_payload = [i for n, i in enumerate(payload) if i not in payload[n + 1:]]
            # pass session to every POST request
            task = asyncio.ensure_future(_post(qm_svc_base_url, member_id, deduped_payload, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        return await responses  # better to explicitly return, allows for storing log of responses if needed in future.


def _get_elation_tracked_qms(qm_svc_base_url, qm_api_key):
    elation_qms_tracked_res = requests.get(f'{qm_svc_base_url}/elationmap', headers={'apiKey': qm_api_key})
    if elation_qms_tracked_res.status_code != 200:
        logging.info(f"\nQM_SERVICE ERROR: {elation_qms_tracked_res.content.decode('utf-8')}") # error is json and so content must be decoded.
        elation_qms_tracked_res.raise_for_status()

    elation_qms_tracked = json.loads(elation_qms_tracked_res.content.decode('utf-8'))

    elation_tags_mapped_qms = {}
    for item in elation_qms_tracked:
        key = item['codeType'] + item['code']  # combo of codeType and code will be unique and not collide.
        elation_tags_mapped_qms[key] = item

    return elation_tags_mapped_qms


def _get_query_str(file_name):
    """ Reads the `.sql` file and returns it as a String object"""
    file = os.path.join(os.path.curdir, 'sql', file_name + '.sql')
    with open(file) as f:
        sql_str = f.read()
    return sql_str


def _run(env: str, qm_api_key: str, project: str):
    """ `main` method for querying elation quality measure data in BQ and posting to QM service

        Datasets queried are cityblock-data.elation_mirror, cbh-db-mirror-prod.commons_mirror and
        cbh-db-mirror-prod.member_index_mirror. The query results go through a light transformation before
        being sent to the QM Service as a post request.
    """

    qm_svc_base_url = _QM_SVC_ENV_BASE_URLS.get(env, _QM_SVC_ENV_BASE_URLS['dev'])

    # get request to QM for list of elation mapped measures.
    elation_tags_mapped_qms = _get_elation_tracked_qms(qm_svc_base_url=qm_svc_base_url, qm_api_key=qm_api_key)

    # run BQ query to get elation tagged qm data.
    client = bigquery.Client(project=project)
    query_file_name = "elation_tags_for_qm_svc"
    query_str = _get_query_str(query_file_name)
    bq_results = client.query(query_str)

    logging.info(f"Query resulted in {bq_results.result().total_rows} rows.")
    member_qms_payload = _generate_member_qms_payload(bq_results=bq_results,
                                                      elation_tags_mapped_qms=elation_tags_mapped_qms)

    logging.info(f'Start posting to QM Service')
    start_time = default_timer()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _async_post_to_qm_service(qm_svc_base_url, member_qms_payload, qm_api_key)
    )
    loop.run_until_complete(future)
    elapsed = default_timer() - start_time
    time_completed_at = "{:5.2f}s".format(elapsed)
    logging.info(f"Post requests to QM Service completed in {time_completed_at} seconds.")


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for Elation tags to QM Service")
    cli.add_argument("--qm-svc-env", type=str, required=True, help="Environment for QM service")
    cli.add_argument("--qm-svc-api-key", type=str, default=os.environ.get("API_KEY_QM"),
                     help="API Key for QM service")
    cli.add_argument("--project", type=str, required=True, help="Project containing GCS data")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    qm_svc_env = args.qm_svc_env  # 'dev' # args.qm_svc_env
    qm_svc_api_key = args.qm_svc_api_key  # 'elation' #  args.qm_svc_api_key
    project_arg = args.project

    _run(env=qm_svc_env, qm_api_key=qm_svc_api_key, project=project_arg)
    logging.info("Elation results successfully posted to QM Service")
