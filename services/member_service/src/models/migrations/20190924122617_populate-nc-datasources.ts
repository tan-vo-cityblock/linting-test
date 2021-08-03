import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const datasources = [
    {
      id: 5,
      name: 'bcbsNC',
    },
    {
      id: 6,
      name: 'medicareNC',
    },
    {
      id: 7,
      name: 'medicaidNC',
    },
  ];

  return knex('datasource').insert(datasources);
}

export async function down(knex: Knex): Promise<any> {
  knex('datasource')
    .whereIn('id', [5, 6, 7])
    .del();
}
