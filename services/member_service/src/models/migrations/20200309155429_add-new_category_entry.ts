import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex('category').insert({
    id: 6,
    name: 'lower risk',
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex('category')
    .where({ id: 6 })
    .del();
}
