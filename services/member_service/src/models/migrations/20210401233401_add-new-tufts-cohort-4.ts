import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 25,
    name: 'Tufts Cohort 4',
    partnerId: 7,
    goLiveDate: '2021-04-01',
    revenueGoLiveDate: '2021-04-01',
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 25 }).del();
}
