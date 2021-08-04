import { Knex } from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.alterTable('hie_message', (table) => {
    table.unique(['messageId']);
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.alterTable('hie_message', (table) => {
    table.dropUnique(['messageId']);
  });
}
