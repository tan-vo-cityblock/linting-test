import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex('partner').insert({ id: 8, name: 'carefirst' });
  await knex('datasource').insert({ id: 23, name: 'carefirst' });
}

export async function down(knex: Knex): Promise<any> {
  await knex('partner').where({ id:  8}).del();
  await knex('datasource').where({ id: 23 }).del();
}
