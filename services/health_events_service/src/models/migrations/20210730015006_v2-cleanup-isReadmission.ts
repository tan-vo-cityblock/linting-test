import { Knex } from 'knex';
import * as hieMessageSignalV2 from '../../db/views/hie_message_signal/v2';

export async function up(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV2.up);
}

export async function down(knex: Knex): Promise<void> {
  return knex.raw(hieMessageSignalV2.down);
}
