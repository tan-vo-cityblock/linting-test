import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const bcbs = [
    {
      id: 3,
      name: 'bcbs_nc',
    },
  ];

  return knex('partner').insert(bcbs);
}

export async function down(knex: Knex): Promise<any> {
  await knex('cohort').where({ partnerId: 3 }).del();
  return knex('partner').where('id', 3).del();
}
