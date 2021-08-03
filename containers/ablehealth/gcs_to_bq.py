import argparse
from collections import namedtuple
import logging
import datetime as dt
from io import BytesIO
from google.cloud.bigquery import schema
import schemas
from typing import List, Dict, NamedTuple, Sequence

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery.schema import SchemaField
from google.api_core.exceptions import NotFound

import helpers

AbleHealthFileManifest = NamedTuple(
    "AbleHealthFileManifest", [("filename", str), ("schema", schemas.BigQuerySchema)])

MANIFESTS = [
    AbleHealthFileManifest("measure_results", schemas.measure_results),
    AbleHealthFileManifest("risk_suspects", schemas.risk_suspects),
    AbleHealthFileManifest("risk_scores", schemas.risk_scores),
]


class ProcessAbleResults:
    """  Takes Able Health data in GCS and puts it into BQ"""

    def __init__(self, gcp_project: str):
        self.project = gcp_project

    def run(self, bucket: str, dataset: str, date: str):
        """ `main` method for retrieving data from GCS and populating it into BQ

        This is done by iterating reading each of the Able Health CSV files as a Pandas DataFrame. This DataFrame then
         undergoes a simple transformation and appended to a table on BQ
        """
        bq_client = bigquery.Client(self.project)
        storage_client = storage.Client(self.project)
        data_bucket = storage_client.get_bucket(bucket)

        for manifest in MANIFESTS:
            file = manifest.filename
            logging.info(f"Retrieving data for {file}")

            # downloading file from GCS and putting into byte stream then reading that into pandas as a dataframe
            blob = data_bucket.get_blob(
                f"ablehealth_results/{date}/{file}.csv")
            byte_stream = BytesIO()
            blob.download_to_file(byte_stream)
            byte_stream.seek(0)
            df = pd.read_csv(byte_stream)
            df.columns = [helpers.convert_snake_to_camel(
                column) for column in df.columns]  # simple transformation
            # adding column for when data was retrieved by Cityblock
            df['createdAt'] = dt.datetime.utcnow()

            # NOTE: pandas_qbg uploads data by uploading csvs
            # bigquery's rest api currently doesn't allow repeated fields
            # in csv uploads
            # so this shouldn't be used, but is kept here for if/when
            # bigquery might support repeated fields in csv uploads
            # 2021-06-21
            for field in manifest.schema.as_schema_fields:
                if field.mode == "REPEATED":
                    df[field.name] = df[field.name].str.strip(
                        '[]').str.split(',')

            logging.info(f"Will now upload {file} data to BigQuery")

            pandas_gbq.to_gbq(
                df,
                f"{dataset}.{file}",
                project_id=self.project,
                table_schema=manifest.schema.as_dicts,
                if_exists="append"
            )  # appending into BQ


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for GCS to BQ")
    cli.add_argument("--project", type=str, required=True,
                     help="BQ project we are reading from")
    cli.add_argument("--bucket", type=str, required=True,
                     help="Bucket to write AH data to")
    cli.add_argument("--dataset", type=str, required=True,
                     help="Bucket to write AH data to")
    cli.add_argument("--date", type=str, required=True,
                     help="Date at which the data is received")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    project = args.project
    bucket = args.bucket
    dataset = args.dataset
    date = args.date

    process_able = ProcessAbleResults(gcp_project=project)

    process_able.run(bucket, dataset, date)
    logging.info("All data successfully loaded into BQ from Able Health")
