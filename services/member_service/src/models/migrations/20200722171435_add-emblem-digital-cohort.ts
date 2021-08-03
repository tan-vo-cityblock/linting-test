import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 21,
    name: 'Emblem Medicaid Digital Cohort 1',
    goLiveDate: '2020-09-01',
    partnerId: 1,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 21 }).del();
}
