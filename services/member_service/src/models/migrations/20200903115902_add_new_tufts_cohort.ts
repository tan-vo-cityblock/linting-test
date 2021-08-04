import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 22,
    name: 'Tufts Cohort 3',
    goLiveDate: '2020-10-01',
    partnerId: 7,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 22 }).del();
}
