import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const negative_cohorts = [
    {
      id: -18,
      name: 'Emblem Cohort -6',
      partnerId: 1,
    },
    {
      id: -20,
      name: 'Emblem Cohort -7',
      partnerId: 1,
    },
  ];
  await knex('cohort').insert(negative_cohorts);
}

export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ id: -18 }).orWhere({ id: -20 }).del();
}
