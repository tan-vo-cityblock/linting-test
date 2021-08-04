import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 24,
    name: 'Emblem Cohort 8',
    goLiveDate: '2021-01-01',
    partnerId: 1,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 24 }).del();
}
