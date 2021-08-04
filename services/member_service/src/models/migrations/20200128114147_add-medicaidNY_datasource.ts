import * as Knex from 'knex';

const medicaidNY = [
  {
    id: 8,
    name: 'medicaidNY',
  },
];

export async function up(knex: Knex): Promise<any> {
  return knex('datasource').insert(medicaidNY);
}

export async function down(knex: Knex): Promise<any> {
  return knex('datasource').where({ id: 8 }).del();
}
