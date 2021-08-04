import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.table('member_demographics', (t) => {
    t.text('race');
    t.text('language');
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.table('member_demographics', (t) => {
    t.dropColumns('race', 'language');
  });
}
