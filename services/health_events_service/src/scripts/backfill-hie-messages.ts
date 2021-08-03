import { BigQuery } from '@google-cloud/bigquery';
import { ResourceStream } from '@google-cloud/paginator';
import { Knex } from 'knex';
import config from '../config';
import { knex } from '../db';

const STAGING_REDOX_MESSAGES_QUERY = `
    SELECT
        patientId,
        eventType,
        payload,
        messageId,
        FROM (
            SELECT
                patientId,
                eventType,
                payload,
                JSON_QUERY(payload, '$.Meta.Message.ID') as messageId
            FROM \`cbh-dan-dewald.raw_redox_for_hes_staging.redox_hie_staging\`
        )
`;

const REDOX_MESSAGES_QUERY = `
    SELECT
        patientId,
        eventType,
        payload,
        messageId
        FROM (
            SELECT
                patient.patientId as patientId,
                eventType,
                payload,
                JSON_QUERY(payload, '$.Meta.Message.ID') as messageId,
                CAST(REPLACE(JSON_QUERY(payload, '$.Meta.EventDateTime'), '\"',"") as TIMESTAMP) as eventDateTime
            FROM \`cityblock-data.streaming.redox_messages\`
        )
    WHERE
        eventDateTime >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) as TIMESTAMP)
    ORDER BY
        eventDateTime DESC;
`;
export const BQ_QUERY_JOB_OPTIONS = {
  query: config.NODE_ENV === 'production' ? REDOX_MESSAGES_QUERY : STAGING_REDOX_MESSAGES_QUERY,
  location: 'US',
};

const getRawRedoxHieMessages = async (bq: BigQuery) => {
  const [job] = await bq.createQueryJob(BQ_QUERY_JOB_OPTIONS);
  return job.getQueryResultsStream();
};

const saveRawRedoxHieMessages = async (messages: ResourceStream<any>, txn: Knex.Transaction) => {
  const records: any[] = [];
  return new Promise((resolve, reject) => {
    messages
      .on('data', async (data) => {
        records.push(data);
      })
      .on('close', async () => {
        const chunkSize = 1000;
        const response = await knex.batchInsert('hie_message', records, chunkSize).transacting(txn);
        resolve(response);
      })
      .on('error', (err) => {
        console.error('Error in saveRawRedoxHieMessages: ', err);
        reject(err);
      });
  });
};

const main = async () => {
  const credentials = JSON.parse(config.GCP_CREDS);
  const bigquery = new BigQuery({ projectId: credentials.project_id, credentials });
  await knex.transaction(async (txn) => {
    // tslint:disable-next-line
    console.log('Getting raw Redox HIE Messages stream...');
    const rawRedoxHieMessageStream = await getRawRedoxHieMessages(bigquery);
    // tslint:disable-next-line
    console.log('Saving raw Redox HIE Messages stream...');
    await saveRawRedoxHieMessages(rawRedoxHieMessageStream, txn);
  });
};

main()
  .then(() => {
    // tslint:disable-next-line
    console.log('Success');
    process.exit(0);
  })
  .catch((err) => {
    // tslint:disable-next-line
    console.error('Error: ', err);
    process.exit(0);
  });
