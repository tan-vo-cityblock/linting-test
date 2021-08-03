import { Knex } from 'knex';

const TABLE_NAME = 'hie_message';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.dropTableIfExists(TABLE_NAME);
  await knex.raw(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
  await knex.schema.createTable(TABLE_NAME, (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('uuid_generate_v4()'));
    table.timestamp('createdAt').defaultTo(knex.raw('now()'));
    table.string('patientId').notNullable();
    table.string('eventType').notNullable();
    table.string('messageId').notNullable();
    table.timestamp('eventDateTime').notNullable();
    table.jsonb('payload').notNullable();
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.dropTableIfExists(TABLE_NAME);
}
