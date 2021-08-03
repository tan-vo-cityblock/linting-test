import * as Knex from "knex";

export async function up(knex: Knex): Promise<any> {
  return knex.schema.table('member', (table) => table.string('zendeskId'));
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('member', (table) => table.dropColumn('zendeskId'));
}
