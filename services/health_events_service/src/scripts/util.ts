import { BigQuery } from '@google-cloud/bigquery';
import { ResourceStream } from '@google-cloud/paginator';
import { Knex } from 'knex';
import config from '../config';
import { knex } from '../db';

export const isProductionEnvironment = config.NODE_ENV === 'production';

export class BigQueryClient {
  client: BigQuery;
  constructor() {
    const credentials = JSON.parse(config.GCP_CREDS);
    this.client = new BigQuery({
      projectId: credentials.project_id,
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery'],
    });
  }
  async getQueryResults(query: string) {
    const [job] = await this.client.createQueryJob(query);
    return job.getQueryResultsStream();
  }
  async saveToDatabase(
    stream: ResourceStream<any>,
    tableName: string,
    txn: Knex.Transaction,
    transform?: (data: any) => any,
  ) {
    const records: any[] = [];
    return new Promise((resolve, reject) => {
      stream
        .on('data', async (data) => {
          const transformedData = transform ? transform(data) : data;
          records.push(transformedData);
        })
        .on('close', async () => {
          const chunkSize = 1000;
          const result = await knex.batchInsert(tableName, records, chunkSize).transacting(txn);
          resolve(result);
        })
        .on('error', (err) => {
          console.error(`Error in BigQueryClient#saveToDatabase`, err);
          reject(err);
        });
    });
  }
  async refreshCacheTable(
    cacheTableName: string,
    stream: ResourceStream<any>,
    transform?: (data: any) => any,
  ) {
    return knex.transaction(async (txn) => {
      await txn.table(cacheTableName).truncate();
      await this.saveToDatabase(stream, cacheTableName, txn, transform);
    });
  }
}
