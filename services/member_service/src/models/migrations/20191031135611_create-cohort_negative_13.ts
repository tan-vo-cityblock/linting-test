import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort').insert({id: -13, name: 'Emblem Cohort -5', partnerId: 1})
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({id: -13}).del
}