from google.cloud import bigquery
import argparse
import logging

def argument_parser():
   cli = argparse.ArgumentParser(description="Argument parser for Combine CCI Provider tables.")
   cli.add_argument("--source_project", type=str, required=True, help="BigQuery project of source gold tables.")
   cli.add_argument(
      "--destination_project",
      type=str,
      required=True,
      help="BigQuery project of destination combined table. Useful for testing."
   )
   cli.add_argument("--amysis_date", type=str, required=True, help="Amysis Provider table date to combine.")
   cli.add_argument("--facets_date", type=str, required=True, help="Facets Provider table date to combine.")
   cli.add_argument("--combined_shard_date", type=str, required=True, help="Shard date of new combined Provider table.")
   cli.add_argument("--bq_job_project", type=str, required=True, help="Project used for permissioning.")

   return cli

if __name__ == "__main__":
   logging.getLogger().setLevel(logging.INFO)

   args = argument_parser().parse_args()

   source_project = args.source_project
   destination_project = args.destination_project
   bq_job_project=args.bq_job_project

   amysis_date = args.amysis_date.replace("-","")
   facets_date = args.facets_date.replace("-","")
   combined_shard_date = args.combined_shard_date.replace("-","")

   client = bigquery.Client(project=bq_job_project)
   sql_query = f"SELECT * FROM {source_project}.gold_claims_incremental_amysis.Provider_{amysis_date} UNION ALL SELECT * FROM " \
      f"{source_project}.gold_claims_facets.Provider_{facets_date} UNION ALL SELECT * FROM " \
      f"{source_project}.gold_claims_dsnp.Provider_20200526"
   job_config = bigquery.QueryJobConfig(
      destination=f"{destination_project}.gold_claims.Provider_{combined_shard_date}",
      write_disposition='WRITE_TRUNCATE'
   )

   query_job = client.query(sql_query, job_config=job_config)

   query_job.result()
   logging.info(f"Created new Provider shard: {destination_project}.gold_claims.Provider_{combined_shard_date}.")
