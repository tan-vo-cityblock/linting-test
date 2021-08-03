import { clean } from 'knex-cleaner';
import { knex } from './src/db';
import { HIE_MESSAGE_SIGNAL_TABLE } from './src/models/hie-message-signal';

beforeEach(async () => {
  await knex.migrate.latest();
});

afterEach(async () => {
  await knex.raw(`DROP MATERIALIZED VIEW ${HIE_MESSAGE_SIGNAL_TABLE}`);
  await clean(knex);
})

afterAll(async () => {
  await knex.destroy();
});