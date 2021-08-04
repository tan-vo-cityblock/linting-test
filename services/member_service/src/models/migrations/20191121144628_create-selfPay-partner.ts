import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('partner').insert({id: 6, name: 'selfPay'})
}

export async function down(knex: Knex): Promise<any> {
    await knex('partner').where({id: 6}).del()
}
