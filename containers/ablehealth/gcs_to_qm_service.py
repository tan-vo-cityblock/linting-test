import argparse
import asyncio
import logging
import os
from datetime import datetime as dt
from io import BytesIO
from timeit import default_timer

import aiohttp
import backoff
import numpy as np
import pandas as pd
import requests
from google.cloud import storage

# TODO - Add typing to improve readability
_ABLE_RESULTS_FILENAME = "measure_results.csv"
_PREV_RESULTS_POST_TO_QM_GCS_URI = f"ablehealth_results_to_qm_service/prev_post_{_ABLE_RESULTS_FILENAME}"

QM_SVC_ENV_BASE_URLS = {
    "prod": "https://quality-measure-dot-cbh-services-prod.appspot.com/1.0",
    "staging": "https://quality-measure-dot-cbh-services-staging.appspot.com/1.0",
    "dev": "http://127.0.0.1:8080/1.0"
}

ABLE_COLS_PARSED = ['external_id', 'rate_id', 'measure_id', 'performance_year',
                    'episode_start_date', 'inverse', 'numerator_performance_met', 'denominator_exclusion']


def _filter_for_qm_tracked_measures(df_able_results, tracked_measures):
    df_reduced = df_able_results[ABLE_COLS_PARSED].copy()
    df_reduced[['rate_id']] = df_reduced[['rate_id']].replace(np.nan, 0).astype(int)

    def filter_df(x):
        return f'{x.measure_id}{x.rate_id}' in tracked_measures.keys()

    return df_reduced[df_reduced[['measure_id', 'rate_id']].apply(filter_df, axis=1)]


def _get_performance_year_start_end(row):
    measure_id = row.measure_id
    performance_year = int(row.performance_year)
    dt_format = '%Y-%m-%d'
    flu_measure_ids = ['FVA', 'FVO']
    if any(flu_id in measure_id for flu_id in flu_measure_ids):
        performance_year_start = dt.strptime(f'{performance_year}-07-01', dt_format)
        next_year = performance_year + 1
        performance_year_end = dt.strptime(f'{next_year}-06-30', dt_format)
    else:
        performance_year_start = dt.strptime(f'{performance_year}-01-01', dt_format)
        performance_year_end = dt.strptime(f'{performance_year}-12-31', dt_format)

    return performance_year_start, performance_year_end

def _derive_qm_status_and_set_on(row, tracked_measures, file_date):

    def format_set_on(set_on_datetime):
        return f'{set_on_datetime.isoformat()}Z'

    performance_year_start, performance_year_end = _get_performance_year_start_end(row)

    performance_file_date = dt.strptime(file_date, '%Y-%m-%d')

    measure_key = f'{row.measure_id}{row.rate_id}'
    tracked_measure_type = tracked_measures[measure_key]['type']

    # if measure type is outcome, setOn should equal file date; otherwise, measure type is 'process'
    # so setOn should equal beginning of performance_year.
    set_on_open_or_excluded = (performance_file_date if tracked_measure_type == 'outcome'
                               else performance_year_start)

    # if file year is performance year, closed will be performance_file_date, otherwise it will be performance_year_end
    is_file_date_within_perf_year = performance_year_start <= performance_file_date <= performance_year_end
    set_on_closed = (performance_file_date if is_file_date_within_perf_year else performance_year_end)

    if row.numerator_performance_met == 0 and row.denominator_exclusion == 1:
        return "excluded", format_set_on(set_on_open_or_excluded)
    elif row.numerator_performance_met == 1 and row.inverse == 0:
        return "closed", format_set_on(set_on_closed)
    elif row.numerator_performance_met == 1 and row.inverse == 1:
        return "open", format_set_on(set_on_open_or_excluded)
    elif row.numerator_performance_met == 0 and row.inverse == 0:
        return "open", format_set_on(set_on_open_or_excluded)
    elif row.numerator_performance_met == 0 and row.inverse == 1:
        return "closed", format_set_on(set_on_closed)
    else:
        raise ValueError(f'Could not determine quality measure status for Able row: {row}')


def _drop_member_id(status_dict):
    del status_dict['memberId']
    return status_dict


def _get_diff_of_able_results(df, df_prev):

    df_reduced = df[ABLE_COLS_PARSED]
    df_prev_reduced = df_prev[ABLE_COLS_PARSED]

    df_diff = df_reduced.merge(df_prev_reduced, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']

    return df_diff


def transform_able_results_to_qm_payload(df, tracked_measures, file_date):

    rows = []

    for row in df.itertuples():
        result_pair = _derive_qm_status_and_set_on(row, tracked_measures, file_date)
        status = result_pair[0]
        set_on = result_pair[1]

        new_row = {
            'memberId': row.external_id,
            'code': row.measure_id,
            'rateId': row.rate_id,
            'status': status,
            'setOn': set_on,
            'performanceYear': int(row.performance_year)
        }

        rows.append(new_row)

    return pd.DataFrame(rows, columns=['memberId', 'code', 'rateId', 'status', 'setOn', 'performanceYear'])


# POST function
@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=7, jitter=backoff.full_jitter)
async def _post(base_url, member_id, member_qms_payload, session):

    member_qm_url = f'{base_url}/member/{member_id}/measures'

    async with session.post(member_qm_url, json=member_qms_payload) as response:
        return await response.text()


async def async_post_to_qm_service(qm_svc_base_url, processed_able_results, qm_api_key):
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
    async with aiohttp.ClientSession(connector=conn, headers={'apiKey': qm_api_key}, raise_for_status=True) as session:

        for member_id, payload in processed_able_results.items():
            member_qms_payload = [{k: v for k, v in d.items() if k != 'memberId'} for d in payload]  # drop memberId

            # pass session to every POST request
            task = asyncio.create_task(_post(qm_svc_base_url, member_id, member_qms_payload, session))
            tasks.append(task)

            # implement a blunt rate limit of 50 async requests at a time to prevent timeout issues when a large number of requests are required.
            if len(tasks) == 50:
                await asyncio.gather(*tasks)
                tasks = []

        # better to explicitly return, allows for storing log of responses if needed in future.
        return await asyncio.gather(*tasks)


def get_tracked_qms(qm_svc_base_url, qm_api_key):
    # get request to QM for list of tracked measures.
    measures_res = requests.get(f'{qm_svc_base_url}/measures', headers={'apiKey': qm_api_key})

    if measures_res.status_code != 200:
        logging.info(f"\nQM_SERVICE ERROR: {measures_res.content.decode('utf-8')}\n") # error can be json or text.
        measures_res.raise_for_status()

    tracked_measures = measures_res.json()

    return {f'{measure["code"]}{measure["rateId"]}': measure for measure in tracked_measures}


def _get_able_results_df(project_name, bucket_name, gcs_uri):
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(gcs_uri)
    byte_stream = BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    return pd.read_csv(byte_stream)


def _write_filtered_able_results_to_gcs(project_name, bucket_name, gcs_uri, df_filtered_able_results):
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(gcs_uri)
    blob.upload_from_string(df_filtered_able_results.to_csv(index=False), content_type='text/csv')


def _run(date: str, env: str, qm_api_key: str, gcs_project: str, gcs_bucket: str):
    """ `main` method for retrieving data from GCS and posting to QM service

    This is done by iterating reading each of the Able Health CSV files as a Pandas DataFrame. This DataFrame then
     undergoes a simple transformation and appended to a table on BQ
    """

    qm_svc_base_url = QM_SVC_ENV_BASE_URLS[env]

    # get request to QM for list of tracked measures.
    tracked_measures = get_tracked_qms(qm_svc_base_url=qm_svc_base_url, qm_api_key=qm_api_key)

    df_prev_able_results = _get_able_results_df(project_name=gcs_project, bucket_name=gcs_bucket,
                                                gcs_uri=_PREV_RESULTS_POST_TO_QM_GCS_URI)

    able_results_uri = f'ablehealth_results/{date}/{_ABLE_RESULTS_FILENAME}'
    df_able_results = _get_able_results_df(project_name=gcs_project, bucket_name=gcs_bucket, gcs_uri=able_results_uri)

    logging.info(f"Row count of Able results: {len(df_able_results.index)}.")

    # filter able results to only grab qms tracked in qm service
    df_filtered_able_results = _filter_for_qm_tracked_measures(df_able_results=df_able_results,
                                                               tracked_measures=tracked_measures)

    logging.info(f"Row count of Able results after filtering for tracked measures: "
                 f"{len(df_filtered_able_results.index)}.")

    logging.info(f"Row count of previous post of filtered able results {len(df_prev_able_results.index)}.")

    # get diff of prev filter of able results and current filter of able results to only post new measures or measures
    # that changed statuses. This is done to significantly reduce post requests to qm service where nothing has changed.
    # this is also done to post new/changed measures only once to fit event log style logic in QM Service.
    df_able_results_diff = _get_diff_of_able_results(df=df_filtered_able_results, df_prev=df_prev_able_results)

    logging.info(f"Row count of diff between prev filtered able results and current filtered results: "
                 f"{len(df_able_results_diff.index)}.")

    # transform able results diff to json schema expected by QM service.
    # Row count of df_able_results_diff === row count of df_transformed_able_results_diff
    df_transformed_able_results_diff = transform_able_results_to_qm_payload(df=df_able_results_diff,
                                                                             tracked_measures=tracked_measures,
                                                                             file_date=date)

    logging.info(f"Row count of transformed diff: {len(df_transformed_able_results_diff.index)}.")

    assert len(df_able_results_diff.index) == len(df_transformed_able_results_diff.index)

    # group qm post payload by memberId
    df_transformed_able_results_diff_grouped = df_transformed_able_results_diff.replace({np.nan: None})\
        .groupby('memberId').apply(lambda x: x.to_dict(orient='records'))

    dict_transformed_able_results_diff_grouped = df_transformed_able_results_diff_grouped.to_dict()

    # post to QM Service
    logging.info(f"Start {len(dict_transformed_able_results_diff_grouped)} post requests to QM Service.")

    start_time = default_timer()
    future = asyncio.run(
        async_post_to_qm_service(qm_svc_base_url, dict_transformed_able_results_diff_grouped, qm_api_key)
    )

    elapsed = default_timer() - start_time
    time_completed_at = "{:5.2f}s".format(elapsed)
    logging.info(f"Post requests to QM Service completed in {time_completed_at} seconds.")

    # write current able results to gcs to overwrite prev posted results. Will be used in future runs to derive diff
    _write_filtered_able_results_to_gcs(project_name=project, bucket_name=gcs_bucket,
                                        gcs_uri=_PREV_RESULTS_POST_TO_QM_GCS_URI,
                                        df_filtered_able_results=df_filtered_able_results)


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for GCS to QM Service")
    cli.add_argument("--able-result-date", type=str, required=True, help="Date at which we recieved AH data")
    cli.add_argument("--qm-svc-env", type=str, required=True, help="Environment for QM service")
    cli.add_argument("--qm-svc-api-key", type=str, default=os.environ.get("API_KEY_QM"), help="API Key for QM service")
    cli.add_argument("--project", type=str, required=True, help="Project containing GCS data")
    cli.add_argument("--bucket", type=str, required=True, help="Bucket containing GCS data")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    able_result_date = args.able_result_date
    qm_svc_env = args.qm_svc_env
    qm_svc_api_key = args.qm_svc_api_key
    project = args.project
    gcs_bucket = args.bucket

    _run(able_result_date, qm_svc_env, qm_svc_api_key, project, gcs_bucket)
    logging.info("Able results successfully posted to QM Service")
