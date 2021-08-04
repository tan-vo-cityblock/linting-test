import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort').insert({id: -99, name: 'Fake Cohort -99', partnerId: 3})
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({id: -99}).del
}