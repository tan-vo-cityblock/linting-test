import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex('cohort').insert({
    id: 14,
    name: 'ConnectiCare Cohort 1d',
    goLiveDate: '2019-12-30',
    partnerId: 2,
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ id: 14 }).del;
}
