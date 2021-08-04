import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const partners = [
    {
      id: 4,
      name: 'medicareNC',
    },
    {
      id: 5,
      name: 'medicaidNC',
    },
  ];

  return knex('partner').insert(partners);
}

export async function down(knex: Knex): Promise<any> {
  return knex('partner')
    .whereIn('id', [4, 5])
    .del();
}
