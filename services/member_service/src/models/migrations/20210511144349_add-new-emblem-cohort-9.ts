import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 26,
    name: 'Emblem Cohort 9',
    partnerId: 1,
    goLiveDate: '2021-05-14',
    revenueGoLiveDate: '2021-05-01',
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 26 }).del();
}
