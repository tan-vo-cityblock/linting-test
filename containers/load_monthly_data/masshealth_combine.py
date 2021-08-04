import argparse
import logging

from google.cloud import bigquery

from get_latest_bq_table_shard import LatestBQTableShard
from update_views import update_view

TUFTS_PROJECT = "tufts-data"
INCREMENTAL_DATASET = "gold_claims_incremental"
COMBINED_DATASET = "gold_claims"
CLAIM_TYPES = ["Professional", "Facility"]
MASSHEALTH_PREFIX = "MassHealth"


def combine_dedup_masshealth(
    latest_bq_table_shard_runner: LatestBQTableShard,
):
    logging.info(
        f"Combining and deduping incremental Masshealth claims data {CLAIM_TYPES}."
    )

    for claim_type in CLAIM_TYPES:
        masshealth_table_name = f"{MASSHEALTH_PREFIX}_{claim_type}"

        latest_incremental_date = latest_bq_table_shard_runner.run(
            dataset=INCREMENTAL_DATASET, required_tables=[masshealth_table_name]
        )

        latest_combined_date = latest_bq_table_shard_runner.run(
            dataset=COMBINED_DATASET, required_tables=[masshealth_table_name]
        )

        logging.info(
            f"Asserting that {COMBINED_DATASET}.{masshealth_table_name} latest date {latest_combined_date} < {INCREMENTAL_DATASET}.{masshealth_table_name} latest date {latest_incremental_date}"
        )
        assert int(latest_combined_date) < int(
            latest_incremental_date
        ), "FAIL! latest combined date must be less than latest incremental date"

        incremental_ref = f"{TUFTS_PROJECT}.{INCREMENTAL_DATASET}.{MASSHEALTH_PREFIX}_{claim_type}_{latest_incremental_date}"
        combined_ref = f"{TUFTS_PROJECT}.{COMBINED_DATASET}.{MASSHEALTH_PREFIX}_{claim_type}_{latest_combined_date}"
        new_combined_ref = f"{TUFTS_PROJECT}.{COMBINED_DATASET}.{MASSHEALTH_PREFIX}_{claim_type}_{latest_incremental_date}"

        logging.info(f"Combining {combined_ref} with {incremental_ref} and deduping")
        # union latest masshealth drop in incremental_gold_claims dataset to
        # deduped latest combined masshealth data in gold_claims dataset
        sql_query = f"""
        WITH
        new_claimIDs AS (SELECT claimID FROM `{incremental_ref}`)
        SELECT * FROM `{combined_ref}`
        WHERE claimID NOT IN (SELECT * FROM new_claimIDs)
        UNION ALL
        SELECT * FROM `{incremental_ref}`
        """

        job_config = bigquery.QueryJobConfig(
            destination=f"{new_combined_ref}",
            write_disposition="WRITE_TRUNCATE",
        )
        query_job = latest_bq_table_shard_runner.client.query(
            sql_query, job_config=job_config
        )
        query_job.result()

        logging.info(
            f"Created new combined {MASSHEALTH_PREFIX}_{claim_type} shard: {new_combined_ref}.\n"
        )


def update_views_combine_tufts_masshealth(
    latest_bq_table_shard_runner: LatestBQTableShard,
):
    logging.info(
        f"Updating views to combine latest Tufts and Masshealth gold claims tables {CLAIM_TYPES}"
    )
    for claim_type in CLAIM_TYPES:
        # Update latest tufts / masshealth shards in gold claims view of latest Facilty / Professional data
        tufts_table_name = claim_type
        latest_combined_date_tufts = latest_bq_table_shard_runner.run(
            dataset=COMBINED_DATASET, required_tables=[tufts_table_name]
        )
        tufts_ref = f"{TUFTS_PROJECT}.{COMBINED_DATASET}.{tufts_table_name}_{latest_combined_date_tufts}"
        logging.info(
            f"{tufts_table_name} table latest_combined_date_tufts: {latest_combined_date_tufts}"
        )

        masshealth_table_name = f"{MASSHEALTH_PREFIX}_{claim_type}"
        latest_combined_date_masshealth = latest_bq_table_shard_runner.run(
            dataset=COMBINED_DATASET, required_tables=[masshealth_table_name]
        )
        masshealth_ref = f"{TUFTS_PROJECT}.{COMBINED_DATASET}.{masshealth_table_name}_{latest_combined_date_masshealth}"
        logging.info(
            f"{masshealth_table_name} table latest_combined_date_masshealth: {latest_combined_date_masshealth}"
        )

        combined_tufts_masshealth_view_query = f"""
        SELECT * FROM `{tufts_ref}`
        UNION ALL
        SELECT * FROM `{masshealth_ref}`
        """

        combined_dataset_ref = latest_bq_table_shard_runner.client.dataset(
            COMBINED_DATASET
        )

        update_view(
            view_name=claim_type,
            dataset_ref=combined_dataset_ref,
            client=latest_bq_table_shard_runner.client,
            sql_query=combined_tufts_masshealth_view_query,
        )
        logging.info(
            f"Combined Tufts and MassHealth {claim_type} gold claims in view named {TUFTS_PROJECT}.{COMBINED_DATASET}.{claim_type}.\n"
        )


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser to combine and de-dup MassHealth Facility and Professional tables."
    )

    cli.add_argument(
        "--update_views_only",
        action="store_true",
        help="Add flag if you only want to update Tufts and MassHealth combined views",
    )

    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = argument_parser().parse_args()

    should_udpate_views_only = args.update_views_only

    latest_bq_table_shard_runner = LatestBQTableShard(TUFTS_PROJECT)

    if should_udpate_views_only:
        update_views_combine_tufts_masshealth(latest_bq_table_shard_runner)
    else:
        combine_dedup_masshealth(latest_bq_table_shard_runner)
        update_views_combine_tufts_masshealth(latest_bq_table_shard_runner)
