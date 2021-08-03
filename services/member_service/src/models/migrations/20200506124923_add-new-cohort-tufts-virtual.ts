import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const cohort = { id: 19, name: 'Tufts Cohort Virtual 1', goLiveDate: '2020-04-29', partnerId: 7 };
  await knex('cohort').insert(cohort);
}

export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ id: 19 }).del();
}
