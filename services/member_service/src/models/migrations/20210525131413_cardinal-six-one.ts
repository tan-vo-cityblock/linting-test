import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex('partner').insert({ id: 9, name: 'cardinal' });
  await knex('datasource').insert({ id: 32, name: 'cardinal' });
  await knex('cohort').insert({
    id: 27,
    name: 'Cardinal Cohort 1',
    goLiveDate: '2021-06-01',
    partnerId: 9,
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex('partner').where({ id: 9 }).del();
  await knex('datasource').where({ id: 32 }).del();
  await knex('cohort').where({ id: 27 }).del();
}