from typing import List, Optional
from urllib import parse

from airflow_utils import prod_link


class LoadPartnerDataConfig:
    """
    Configuration and utility functions for partner ingestion dags.
    """

    def __init__(
        self,
        bq_project: str,
        silver_dataset: str,
        gold_dataset: str,
        combined_gold_dataset: Optional[str] = None,
        flattened_gold_dataset: Optional[str] = "gold_claims_flattened",
    ):
        self.bq_project = bq_project
        self.silver_dataset = silver_dataset
        self.gold_dataset = gold_dataset
        self.combined_gold_dataset = combined_gold_dataset
        self.flattened_gold_dataset = flattened_gold_dataset

    def _all_datasets(self) -> List[str]:
        datasets = [self.silver_dataset, self.gold_dataset]
        if self.combined_gold_dataset:
            datasets.append(self.combined_gold_dataset)
        return datasets

    def _parse_errors_url(self, shard: str) -> str:
        """
        Returns link to this job's ParseErrors table in the BigQuery UI
        :param shard: BQ shard for this load job
        """
        base = "https://console.cloud.google.com/bigquery?"
        query_args = {
            "p": self.bq_project,
            "d": self.silver_dataset,
            "t": "ParseErrors_{shard}".format(shard=shard),
            "page": "table",
        }
        return base + parse.urlencode(query_args)

    def _delete_silver_tables_command(self, shard: str) -> str:
        """
        Returns a string representation of a shell command that deletes all silver and gold tables loaded by this job.
        :param shard: BQ shard for this load job
        """
        return """
        bq --project_id={project} ls -n 1000000 --format=csv {dataset} | grep {shard} \
        | grep -v ParseErrors | cut -d, -f2 \
        | xargs -n1 bq --project_id={project} rm -f t
        """.format(
            project=self.bq_project, dataset=self.silver_dataset, shard=shard
        )

    def result_email_html(self, description: str, dag_id: str, shard: str) -> str:
        """
        Returns a simple html message indicating this job's completion along with links to QA artifacts.
        :param description: short description of job
        :param dag_id: airflow dag id
        :param shard: BQ shard for this load job
        """
        ge_results_url = "https://data-docs.cityblock.com/ge"

        return """
        <html>
        <p>{description} job is complete. The relevant tables should be visible in BigQuery and are
        ready for QA.</p>
        <p>Links:
          <ul>
            <li><a href="{dag_url}">View the DAG here</a></li>
            <li><a href="{parse_errors_url}">View ParseErrors here</a></li>
            <li><a href="{ge_results_url}">View the Great Expectations QA results here.</a></li>
          </ul>
        </p>
        <p>To reload data, run the following command to delete the newly-loaded silver data,
        then trigger the DAG manually. When you do so, the DAG will overwrite any existing gold
        tables for this shard.
        <pre>{delete_tables_command}</pre></p>
        <p>This is an automated email, please do not reply to this.</p>
        </html>
        """.format(
            description=description,
            dag_url=prod_link.format(dag_id=dag_id),
            parse_errors_url=self._parse_errors_url(shard),
            ge_results_url=ge_results_url,
            delete_tables_command=self._delete_silver_tables_command(shard),
        )
