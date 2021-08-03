import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const tuftsInsurance = [
    {
      id: 20,
      name: 'tufts',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'MCO',
    },
    {
      id: 21,
      name: 'tufts',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'direct',
    },
  ];

  return knex.table('datasource').insert(tuftsInsurance);
}

export async function down(knex: Knex): Promise<any> {
  await knex.table('datasource').select().whereBetween('id', [20, 21]).del();
}
