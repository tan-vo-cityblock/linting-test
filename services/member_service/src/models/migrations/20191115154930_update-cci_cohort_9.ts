import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort')
    .where({ id: 9 })
    .update({ name: 'ConnectiCare Cohort 1b', goLiveDate: '2019-05-30'});    
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort')
    .where({ id: 9 })
    .update({ name: 'ConnectiCare Cohort 1c',  goLiveDate: '2019-07-08' });
}