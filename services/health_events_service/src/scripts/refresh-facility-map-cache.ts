import { isProductionEnvironment, BigQueryClient } from './util';

const FACILITY_MAP_DATASET_ID = isProductionEnvironment
  ? `reference-data-199919.facility.facility_map`
  : `cbh-dan-dewald.hes_facility_map_staging.staging_reference_data_facility_map`;
const query = `SELECT * FROM ${FACILITY_MAP_DATASET_ID}`;
const TABLE_NAME = 'facility_map_cache';

const transform = (data: any) => ({
  ...data,
  createdAt: data.createdAt.value,
  lastModified: data.lastModified.value,
});

const main = async () => {
  const bq = new BigQueryClient();
  const stream = await bq.getQueryResults(query);
  await bq.refreshCacheTable(TABLE_NAME, stream, transform);
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
