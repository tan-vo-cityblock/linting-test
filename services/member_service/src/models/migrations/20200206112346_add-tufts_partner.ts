import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('partner').insert({id: 7, name: 'tufts'});
    await knex('datasource').insert({id: 9, name: 'tufts'});
}

export async function down(knex: Knex): Promise<any> {
    await knex('partner').where({id: 7}).del();
    await knex('datasource').where({id: 9}).del();
}

