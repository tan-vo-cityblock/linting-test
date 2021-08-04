import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.createTable('member_eligibilities', (table) => {
    table.uuid('id').unique().defaultTo(knex.raw('uuid_generate_v4()'));
    table.string('commonsHarpStatus');
    table.uuid('memberId').references('id').inTable('member').notNullable();
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('member_eligibilities');
}
