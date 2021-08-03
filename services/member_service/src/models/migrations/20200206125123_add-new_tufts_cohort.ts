import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const cohort = { id: 15, name: 'Tufts Cohort 1', goLiveDate: '2020-03-02', partnerId: 7 };
  await knex('cohort').insert(cohort);
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({id: 15}).del();
}
