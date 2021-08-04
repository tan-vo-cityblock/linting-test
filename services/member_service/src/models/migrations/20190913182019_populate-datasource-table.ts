import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const datasources = [
    {
      id: 1,
      name: 'emblem',
    },
    {
      id: 2,
      name: 'acpny',
    },
    {
      id: 3,
      name: 'connecticare',
    },
    {
      id: 4,
      name: 'elation',
    },
  ];

  return knex('datasource').insert(datasources);
}

export async function down(knex: Knex): Promise<any> {
  return knex('datasource').del();
}
