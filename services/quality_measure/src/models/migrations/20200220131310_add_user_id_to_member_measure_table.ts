import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.table('member_measure', (table) => {
    table.uuid('userId');
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.table('member_measure', (table) => {
    table.dropColumn('userId');
  });
}
