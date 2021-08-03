import * as Knex from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable('member_demographics', (table) => {
    table.boolean('isMarkedDeceased').defaultTo(false);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable('member_demographics', (table) => {
    table.dropColumn('isMarkedDeceased');
  });
}
