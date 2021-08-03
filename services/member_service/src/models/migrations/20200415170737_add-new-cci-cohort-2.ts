import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert({
    id: 17,
    name: 'ConnectiCare Cohort 2',
    goLiveDate: '2020-05-15',
    partnerId: 2,
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort')
    .where({ id: 17 })
    .del();
}
