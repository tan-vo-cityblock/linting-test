import Knex from 'knex';
import { Model } from 'objection';
import * as knexConfig from '../models/knexfile';

export const setupDb = () => {
  const config = (knexConfig as { [key: string]: any }).test;
  const knex = Knex(config);
  Model.knex(knex);

  return {
    destroy: async () => {
      try {
        await knex.destroy();
      } catch (err) {
        console.error(`inside test-utils: ${err.message}`);
      }
    },
  };
};
