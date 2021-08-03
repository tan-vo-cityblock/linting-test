import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const tuftsInsurance = {
    id: 22,
    name: 'connecticare',
    lineOfBusiness: 'medicare',
    subLineOfBusiness: 'dual',
  };

  return Promise.all([
    knex('cohort').where({ id: 17 }).update({ goLiveDate: '2020-06-01' }),
    knex.table('datasource').insert(tuftsInsurance),
  ]);
}

export async function down(knex: Knex): Promise<any> {
  return Promise.all([
    knex('cohort').where({ id: 17 }).update({ goLiveDate: '2020-05-15' }),
    knex.table('datasource').select().where('id', 22).del(),
  ]);
}
