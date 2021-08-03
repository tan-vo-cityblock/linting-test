import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 3 }).select().update({ name: 'Emblem Cohort 3a' });
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').where({ id: 3 }).select().update({ name: 'Emblem Cohort 3' });
}
