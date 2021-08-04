import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const categories = [
    {
      id: 1,
      name: 'high cost',
    },
    {
      id: 2,
      name: 'high risk',
    },
    {
      id: 3,
      name: 'rising risk',
    },
    {
      id: 4,
      name: 'HARP',
    },
    {
      id: 5,
      name: 'removed',
    },
  ];

  return knex('category').insert(categories);
}

export async function down(knex: Knex): Promise<any> {
  return knex('category').del();
}
