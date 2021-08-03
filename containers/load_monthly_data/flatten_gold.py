import logging
from argparse import ArgumentParser
from typing import Optional

from google.cloud import bigquery

import query_templates

logging.getLogger().setLevel(logging.INFO)

SUPPORTED_TABLES = ['Facility', 'Professional', 'Pharmacy']
SUPPORTED_PARTNERS = ['cci', 'emblem', 'tufts', 'emblem_virtual', 'carefirst']


class GoldFlattener:
    """
    Creates flattened representation of gold tables for a given partner and date shard.
    """

    def __init__(self, bq_project: str, partner: str, input_project: str, input_dataset: str,
                 output_project: str, output_dataset: str, shard: str):
        """
        Construct a new GoldFlattener.

        :param bq_project: project to run BQ jobs in
        :param partner:
        :param input_project: BQ project for reading "nested" claims
        :param input_dataset: BQ dataset for reading "nested" claims
        :param output_project: BQ project to write flattened claims
        :param output_dataset: BQ dataset to write flattened claims
        :param shard: YYYYMMdd-formatted date that doubles as BQ table suffix
        """
        self.client = bigquery.Client(project=bq_project)
        self.partner = partner
        self.input_project = input_project
        self.input_dataset = input_dataset
        self.output_project = output_project
        self.output_dataset = output_dataset
        self.shard = shard

    @staticmethod
    def _build_table_reference(project: str, dataset: str, table: str,
                               shard: Optional[str], quote: bool = False) -> str:
        """
        Return reference to the given table as a string to be used in a sql query or BQ API command.
        """
        full_table_name = f"{table}_{shard}" if shard else table
        ref = f"{project}.{dataset}.{full_table_name}"
        if quote:
            ref = f"`{ref}`"
        return ref

    def _get_sql(self, table: str) -> str:
        """
        Return SQL query that produces flattened version of the table.

        :param table: gold table to flatten
        """
        ref = GoldFlattener._build_table_reference(self.input_project,
                                                   self.input_dataset,
                                                   table,
                                                   self.shard, quote=True)
        if table == 'Facility':
            return query_templates.flatten_gold_facility(table_reference=ref, partner=self.partner)
        elif table == 'Professional':
            return query_templates.flatten_gold_professional(table_reference=ref,
                                                             partner=self.partner)
        elif table == 'Pharmacy':
            return query_templates.flatten_gold_pharmacy(table_reference=ref, partner=self.partner)
        else:
            raise Exception('Table "{table}" is unsupported. Must be one of {supported}.'.format(
                table=table,
                supported=', '.join(SUPPORTED_TABLES)))

    def query_job(self, table: str) -> bigquery.QueryJob:
        """
        Creates a new BQ QueryJob to generate and write a flattened version of the gold table.

        This function doesn't actually submit the job to the BigQuery API. To submit the job and
        wait for the result:
        code-block::
            job = flattener.query_job(...)
            job.result()

        :param table: table to flatten
        """
        sql = self._get_sql(table)
        output_table_reference = GoldFlattener._build_table_reference(self.output_project,
                                                                      self.output_dataset,
                                                                      table,
                                                                      self.shard)
        job_config = bigquery.QueryJobConfig(destination=output_table_reference,
                                             use_legacy_sql=False,
                                             write_disposition='WRITE_TRUNCATE')
        job_id_prefix = f'flatten_gold_{partner}_{table.lower()}_'
        return self.client.query(sql, job_config=job_config, job_id_prefix=job_id_prefix)


def _argparser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--partner', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--input_project', required=True)
    parser.add_argument('--input_dataset', required=True)
    parser.add_argument('--output_project', required=True)
    parser.add_argument('--output_dataset', required=True)
    parser.add_argument('--shard', required=True)
    parser.add_argument('--tables', required=True)
    return parser


if __name__ == "__main__":
    args = _argparser().parse_args()
    partner = args.partner
    project = args.project
    input_project = args.input_project
    input_dataset = args.input_dataset
    output_project = args.output_project
    output_dataset = args.output_dataset
    shard = args.shard

    if partner not in SUPPORTED_PARTNERS:
        logging.error('Partner "{partner}" is invalid. Must be one of {supported}.'.format(
            partner=partner, supported=', '.join(SUPPORTED_PARTNERS))
        )
        exit(1)

    flattener = GoldFlattener(bq_project=project, partner=partner, input_project=input_project,
                              input_dataset=input_dataset, output_project=output_project,
                              output_dataset=output_dataset, shard=shard)
    jobs = []
    for t in args.tables.split(','):
        if t not in SUPPORTED_TABLES:
            logging.warning(f'Table "{t}" is not supported. Skipping.')
            continue
        job = flattener.query_job(t)
        logging.info(f'Submitted query job {job.job_id}.')
        jobs.append(job)

    for job in jobs:
        # Block until job completes. Throws an exception if the job encountered any errors.
        job.result()
        logging.info(f'query job {job.job_id} completed.')
