import argparse
import logging
import os

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import helpers


class ProcessQReviewsResults:
    """Hits Q-Review endpoints to retrieve data available for Cityblock and populated those results into BQ"""

    def __init__(self, project: str, dataset: str, api_account_sid: str, api_key: str):
        self.base_url = "https://qrev.ws/api/client/v2/response"
        self.session = self._create_session(api_account_sid, api_key)

        self.project = project
        self.dataset = dataset
        self.table = 'qreviews_results'

        self.bigquery_client = bigquery.Client(project=project)

        self.dataset_ref = self.bigquery_client.dataset(self.dataset)
        self.table_ref = self.dataset_ref.table(self.table)

    def run(self):
        """ `main` method for retrieving data from Able Health and populating it into BQ

        This is done by requesting data from the main endpoint with the sequence number paramater,
        sending a GET request and loading the contents into bigquery.
        """

        self._create_dataset_if_not_exits()
        self._check_if_table_exists()

        max_sequence = self._get_max_sequence()
        response_data = self._get_data(max_sequence)

        # if no rows are returned response data is an empty list ie []
        if response_data:
            response_data = helpers.keymap_nested_dicts(
                response_data, helpers.convert_snake_to_camel)
            self._stream_to_bq(response_data)
        else:
            logging.info(f"No new data, nothing uploaded to BigQuery")

    @staticmethod
    def _create_session(api_account_sid: str, api_key: str):
        session = requests.Session()
        session.headers.update(
            {'Authorization': f'ApiKey {api_account_sid}:{api_key}'})
        return session

    def _create_dataset_if_not_exits(self):
        try:
            self.bigquery_client.get_dataset(self.dataset_ref)
        except NotFound as err:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"

            # Make an API request.
            self.bigquery_client.create_dataset(dataset)
            logging.info(f"Created dataset {self.dataset_ref}")

    def _check_if_table_exists(self):
        try:
            self.bigquery_client.get_table(self.table_ref)
        except NotFound as err:
            logging.error(
                f"""
                Error: {err}\n\nFIX: Make sure table is created and managed in Terraform where schema is defined.
                If you are running this in development, copy table `cityblock-data.qreviews.qreviews_results` to your personal project via the console
                """)
            raise

    def _get_max_sequence(self):
        results_table = f"{self.project}.{self.dataset}.{self.table}"
        query = f"select max(sequence) from `{results_table}`"

        query_job = self.bigquery_client.query(query)
        max_found = None
        for row in query_job:
            max_found = row[0]

        if not max_found:
            max_found = 0
            logging.info(f"No rows found in result table: set max_found to 0")
        else:
            logging.info(
                f"Max sequence number found in {results_table}: set max_found to {max_found}")

        return max_found

    def _get_data(self, max_sequence: str):
        response_data = []
        number_of_results = 500

        while number_of_results == 500:
            logging.info(
                f"Start: get data for sequence_gt: {max_sequence}, limit: 500")
            url = self.base_url + \
                f'?&sequence__gt={max_sequence}&order_by=sequence&limit=500&omit_count=true'
            req = self.session.get(url=url)

            if req.status_code != 200:
                logging.info(
                    f"\nQREVIEWS TO CBH ERROR: {req.content.decode('utf-8')}\n")
                # stops execution of while loop and script if qreviews responds with an error
                req.raise_for_status()

            logging.info(
                f"Success: get data for sequence_gt: {max_sequence}, limit: 500")
            req_data = req.json()
            req_data = req_data['objects']

            number_of_results = len(req_data)

            if number_of_results > 0:

                max_sequence = req_data[-1]['sequence']
                response_data.extend(req_data)
            else:
                logging.info(f"Call returned 0 objects")

        logging.info(f"Row count of qreviews results: {len(response_data)}")
        return response_data

    def _stream_to_bq(self, data):
        table = self.bigquery_client.get_table(self.table_ref)
        all_errors = []
        # arbitrarily chosen chunk size, can play with this to optimize for speed of process
        chunk_size = 250

        for chunked_data in list(helpers.chunks(data, chunk_size)):
            # Make an API request.
            errors = self.bigquery_client.insert_rows_json(table, chunked_data)
            all_errors.extend(errors)

        if not all_errors:
            logging.info(f"{len(data)} new rows have been added.")
        else:
            logging.error(f"Errors in load job {all_errors}.")
            raise RuntimeError(
                f'FAILURE: Errors were recorded during processing of QReviews results. See logs above.')

        logging.info("Loading job finished.")


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser for Q-Reviews to BQ")
    cli.add_argument("--project", type=str, required=True,
                     help="BQ project we are reading from and writing to")
    cli.add_argument("--dataset", type=str, required=True,
                     help="Dataset to write Qreviews data to")
    cli.add_argument("--api-account-sid", type=str,
                     default=os.environ.get("API_ACCOUNT_SID"), help="Qreviews api account id")
    cli.add_argument("--api-key", type=str,
                     default=os.environ.get("API_KEY"), help="Qreviews api account key")
    return cli


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    project = args.project
    dataset = args.dataset
    api_account_sid = args.api_account_sid
    api_key = args.api_key

    process_qreviews = ProcessQReviewsResults(
        project, dataset, api_account_sid, api_key)
    process_qreviews.run()
    logging.info("Script run successfully")
