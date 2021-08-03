import * as Knex from "knex";

export async function up(knex: Knex): Promise<any> {
  const carefirstInsurance = {
    id: 24,
    name: 'carefirst',
    lineOfBusiness: 'medicaid',
    subLineOfBusiness: 'supplemental security income',
  };

  return knex.table('datasource').insert(carefirstInsurance);
}


export async function down(knex: Knex): Promise<any> {
  return knex.table('datasource').select().where('id', 24).del();
}

