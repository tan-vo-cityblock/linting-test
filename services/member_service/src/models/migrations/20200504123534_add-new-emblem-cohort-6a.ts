import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex('cohort').insert({
    id: 18,
    name: 'Emblem Cohort 6a',
    partnerId: 1,
    goLiveDate: '2020-05-01',
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ id: 18 }).del();
}
