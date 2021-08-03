import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 20,
    name: 'Emblem Cohort 7',
    goLiveDate: '2020-07-01',
    partnerId: 1,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 20 }).del();
}
