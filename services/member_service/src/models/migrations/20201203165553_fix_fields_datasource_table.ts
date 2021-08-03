import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return Promise.all([
    knex('datasource').where({ id: 26 }).update({ name: 'medicaidCT' }),
    knex('datasource').where({ id: 27 }).update({ name: 'medicareCT' }),
    knex('datasource').where({ id: 30 }).update({ name: 'medicaidDC' }),
    knex('datasource').where({ id: 31 }).update({ name: 'medicareDC' }),
    knex('datasource').where({ id: 15 }).update({ subLineOfBusiness: 'fully insured' }),
    knex('datasource').where({ id: 16 }).update({ subLineOfBusiness: 'fully insured' }),
  ]);
}

export async function down(knex: Knex): Promise<any> {
  return Promise.all([
    knex('datasource').where({ id: 26 }).update({ name: 'medicaidCI' }),
    knex('datasource').where({ id: 27 }).update({ name: 'medicareCI' }),
    knex('datasource').where({ id: 30 }).update({ name: 'medicaidMD' }),
    knex('datasource').where({ id: 31 }).update({ name: 'medicareMD' }),
    knex('datasource').where({ id: 15 }).update({ subLineOfBusiness: 'fully exchanged' }),
    knex('datasource').where({ id: 16 }).update({ subLineOfBusiness: 'fully insured off exchange' }),
  ]);
}
