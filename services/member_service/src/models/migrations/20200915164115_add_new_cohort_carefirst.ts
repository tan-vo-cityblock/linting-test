import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 23,
    name: 'CareFirst Cohort 1',
    goLiveDate: '2020-10-01',
    partnerId: 8,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 23 }).del();
}
