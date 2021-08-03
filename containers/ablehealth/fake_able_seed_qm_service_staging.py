import argparse
import asyncio
import logging
import random
import os
from timeit import default_timer
from datetime import datetime as dt

import numpy as np
import pandas as pd
from google.cloud import bigquery
from gcs_to_qm_service import get_tracked_qms, ABLE_COLS_PARSED, QM_SVC_ENV_BASE_URLS, transform_able_results_to_qm_payload, async_post_to_qm_service

def _get_commons_staging_member_ids(project_name):
    # run BQ query to get common's staging member ids.
    client = bigquery.Client(project=project_name)

    query = """
        SELECT id FROM `cbh-db-mirror-staging.commons_mirror.patient`
    """
    query_job = client.query(query)
    rows = query_job.result()
    logging.info(f"Query resulted in {rows.total_rows} commons staging member ids.")
    member_ids = [row.get('id') for row in list(rows)]
    return member_ids

def mk_fake_able_data(staging_member_ids, tracked_measures, file_date):
    performance_file_date = dt.strptime(fake_file_date, '%Y-%m-%d')
    rows = []
    for staging_id in staging_member_ids:
        potential_measures = random.sample(tracked_measures, len(tracked_measures))
        for i in range(len(tracked_measures)):
            if not potential_measures: continue
            measure = potential_measures.pop()
            measure = random.choice([measure, None])

            if not measure: continue

            measure_id = measure.get('code')
            rate_id = measure.get('rateId')

            if 'FV' in measure_id:
                potential_measures = [x for x in potential_measures if 'FV' not in x.get('code')]

            if 'AWV' in measure_id:
                potential_measures = [x for x in potential_measures if 'AWV' not in x.get('code')]


            # set performance_year for flu (FV) to be prior year, otherwise performance year is year of fake file date.
            performance_year = (performance_file_date.year - 1) if 'FV' in measure_id else performance_file_date.year

            row = {
                'external_id': staging_id,
                'rate_id': rate_id,
                'measure_id': measure_id,
                'performance_year': performance_year,
                'episode_start_date': None,
                'inverse': random.choice([0, 1]),
                'numerator_performance_met': random.choice([0, 1]),
                'denominator_exclusion': random.choice([0, 0, 0, 0, 1])
            }
            rows.append(row)
    return pd.DataFrame(rows, columns=ABLE_COLS_PARSED)


def _run(env: str, qm_api_key: str, project: str, fake_file_date: str):
    qm_svc_base_url = QM_SVC_ENV_BASE_URLS[env]

    tracked_measures = get_tracked_qms(qm_svc_base_url=qm_svc_base_url, qm_api_key=qm_api_key)
    tracked_measure_vals = list(tracked_measures.values())
    logging.info(f"Pulled {len(tracked_measures)} tracked measures from QM Service {env}.")

    staging_member_ids = _get_commons_staging_member_ids(project_name=project)
    logging.info(f"Pulled {len(staging_member_ids)} Commons Staging member IDs from BQ Mirror.")

    tracked_measure_vals = list(tracked_measures.values())

    df_fake_able = mk_fake_able_data(staging_member_ids, tracked_measure_vals, file_date=fake_file_date)


    df_transformed_able_results = transform_able_results_to_qm_payload(df=df_fake_able,
                                                                             tracked_measures=tracked_measures,
                                                                             file_date=fake_file_date)

    logging.info(f"Row count of transformed df: {len(df_transformed_able_results.index)}.")

    assert len(df_fake_able.index) == len(df_transformed_able_results.index)

    # group qm post payload by memberId
    df_transformed_able_results_grouped = df_transformed_able_results.replace({np.nan: None})\
        .groupby('memberId').apply(lambda x: x.to_dict(orient='records'))

    dict_transformed_able_results_grouped = df_transformed_able_results_grouped.to_dict()

    # post to QM Service
    logging.info(f"Start {len(dict_transformed_able_results_grouped)} post requests to {env} QM Service.")

    start_time = default_timer()
    future = asyncio.run(
        async_post_to_qm_service(qm_svc_base_url, dict_transformed_able_results_grouped, qm_api_key)
    )

    elapsed = default_timer() - start_time
    time_completed_at = "{:5.2f}s".format(elapsed)
    logging.info(f"Post requests to QM Service {env} completed in {time_completed_at} seconds.")

def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for to seed QM Staging Service with fake Able data")
    cli.add_argument("--fake-able-result-date", type=str, required=True, help="Fake date (YYYY-MM-DD) at which we recieved AH data")
    cli.add_argument("--qm-svc-env", type=str, default="dev", help="Environment for QM service")
    cli.add_argument("--qm-svc-api-key", type=str, default=os.environ.get("API_KEY_QM"), help="API Key for QM service")
    cli.add_argument("--project", type=str, required=True, help="Project containing GCS data")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # # Parse input arguments
    args = argument_parser().parse_args()

    # DO NOT RUN IN PROD
    assert args.qm_svc_env != "prod"

    qm_svc_env = args.qm_svc_env
    qm_svc_api_key = args.qm_svc_api_key
    project = args.project
    fake_file_date = args.fake_able_result_date

    _run(qm_svc_env, qm_svc_api_key, project, fake_file_date)

    logging.info("Masked Able results successfully posted to QM Service Staging")
