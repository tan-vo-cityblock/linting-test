import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort')
      .where({ name: 'Emblem Cohort 4b' })
      .update({ goLiveDate: '2019-08-28' });
  }
  
  export async function down(knex: Knex): Promise<any> {
    await knex('cohort')
      .where({ name: 'Emblem Cohort 4b' })
      .update({ goLiveDate: null });
  }
  