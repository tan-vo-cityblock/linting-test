import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('cohort').insert([
    {
      id: 12,
      name: 'Emblem Cohort 4b',
      // assignmentDate is currently unknown
      partnerId: 1,
    },
  ]);
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort')
    .where('id', 12)
    .del();
}
