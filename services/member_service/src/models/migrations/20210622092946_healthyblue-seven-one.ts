import * as Knex from "knex";


export async function up(knex: Knex): Promise<any> {
    await knex('partner').insert({ id: 10, name: 'Healthy Blue' });
    await knex('cohort').insert({
      id: 28,
      name: 'Healthy Blue 1',
      goLiveDate: '2021-07-01',
      partnerId: 10,
    });
  }
  
  export async function down(knex: Knex): Promise<any> {
    await knex('partner').where({ id: 10 }).del();
    await knex('cohort').where({ id: 28 }).del();
  } 

