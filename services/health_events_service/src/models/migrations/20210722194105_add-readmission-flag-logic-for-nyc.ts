import { Knex } from 'knex';
import * as hieMessageSignalV1 from '../../db/views/hie_message_signal/v1';

export async function up(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV1.up);
}

export async function down(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV1.down);
}
