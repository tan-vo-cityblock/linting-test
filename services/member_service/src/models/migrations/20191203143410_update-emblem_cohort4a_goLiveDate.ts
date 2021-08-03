import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort').where({id: 11}).update({goLiveDate: '2019-07-25'})
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({id: 11}).update({goLiveDate: '2019-07-22'})
}
