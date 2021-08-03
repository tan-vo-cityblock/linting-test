import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const partners = [
    {
      id: 1,
      name: 'emblem',
    },
    {
      id: 2,
      name: 'connecticare',
    },
  ];

  return knex('partner').insert(partners);
}

export async function down(knex: Knex): Promise<any> {
  return knex('partner').del();
}
