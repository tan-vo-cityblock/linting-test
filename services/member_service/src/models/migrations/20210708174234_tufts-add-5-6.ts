import * as Knex from "knex";


export async function up(knex: Knex): Promise<any> {
  await knex('cohort').insert({
    id: 29,
    name: 'Tufts Cohort 5',
    goLiveDate: '2021-08-01',
    partnerId: 7,
  });
  await knex('cohort').insert({
    id: 30,
    name: 'Tufts Cohort 6',
    goLiveDate: '2021-08-01',
    partnerId: 7,
  });
}


export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ id: 29 }).del();
  await knex('cohort').where({ id: 30 }).del();
}

