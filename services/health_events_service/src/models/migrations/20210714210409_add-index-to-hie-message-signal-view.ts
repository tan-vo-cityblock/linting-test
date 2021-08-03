import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.raw(`CREATE UNIQUE INDEX hie_message_id_index ON hie_message_signal("messageId");`);
  return Promise.resolve();
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(`DROP INDEX IF EXISTS hie_message_id_index;`);
  return Promise.resolve();
}
