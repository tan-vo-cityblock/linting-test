import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort').insert({ id: -6, name: 'ConnectiCare Cohort 1 Supplementary 20190314', partnerId: 2});
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort').where({ id: -6}).del;
}