import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex('cohort')
    .where({ id: 6 })
    .update({ name: 'ConnectiCare Cohort 1 Supplementary 20190314' });    
}

export async function down(knex: Knex): Promise<any> {
    await knex('cohort')
    .where({ id: 6 })
    .update({ name: 'ConnectiCare Cohort 1b' });
}