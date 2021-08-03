import * as Knex from 'knex';


export async function up(knex: Knex): Promise<any> {
    await knex('cohort').insert({ id: 13, name: 'Emblem Cohort 5a', partnerId: 1, goLiveDate: '2019-11-04' });
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({ name: 'Emblem Cohort 5a'}).del;
}
