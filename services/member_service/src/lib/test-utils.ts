import Knex from 'knex';
import { Model, Transaction } from 'objection';
import * as knexConfig from '../models/knexfile';

export const destroyDb = async () => {
  try {
    await Model.knex().destroy();
  } catch (err) {
    console.error(`inside test-utils: ${err.message}`);
  }
};

export const setupDb = () => {
  const config = (knexConfig as { [key: string]: any }).test;
  const knex = Knex(config);
  Model.knex(knex);
  return { destroy: destroyDb };
};

export const mockTxn = (undefined as unknown) as Transaction;

interface KnexCleanerOptions {
  ignoreTables?: string[];
  mode?: 'truncate' | 'delete';
  restartIdentity?: boolean;
}

// Truncates tables in the database between tests preserving tables that have
// data populated from migrations.
//
// TODO: De-couple schema migrations from database seeding so that we can
// run `knex.seed.run()` between tests as explained here:
// https://github.com/knex/knex/issues/2076#issuecomment-448996123
//
// "Using transaction to reset test data is generally a bad idea and I would
// even consider it as an anti-pattern. So is overriding knex internals.
// Truncate + repopulate db on every test or for set of tests is recommendation"
// ~ Knex maintainers
// https://github.com/knex/knex/issues/2076#issuecomment-498246985
//
export const cleanDb = async ({
  ignoreTables = [
    'category',
    'cohort',
    'damm_matrix',
    'datasource',
    'elation_map',
    'market_measure',
    'market',
    'measure_category',
    'measure',
    'member_measure_source',
    'member_measure_status',
    'partner',
  ],
} = {}) => {
  const {
    clean,
  }: {
    clean: (knex: Knex, options?: KnexCleanerOptions) => Promise<void>;
  } = require('knex-cleaner');
  return clean(Model.knex(), { ignoreTables });
};
