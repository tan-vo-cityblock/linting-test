import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const memberDatasource = knex.schema.table('member_insurance', (t) => {
    t.dropColumn('spanDateStart');
    t.dropColumn('spanDateEnd');
  });

  const datasource = knex.schema
    .table('datasource', (t) => {
      t.dropUnique(['name', 'lineOfBusiness', 'subLineOfBusiness']);
      t.dropColumn('lineOfBusiness');
      t.dropColumn('subLineOfBusiness');
    })
    .then(async () => {
      return knex.schema.table('datasource', (t) => {
        t.unique(['name'], 'index_unique_carrier_name');
      });
    });

  await Promise.all([memberDatasource, datasource]);
  return knex.raw('DROP TYPE insurance_lob');
}

export async function down(knex: Knex): Promise<any> {
  const memberDatasource = knex.schema.table('member_insurance', (t) => {
    t.date('spanDateStart');
    t.date('spanDateEnd');
  });
  const datasource = knex.schema
    .table('datasource', (t) => {
      t.dropUnique(['name'], 'index_unique_carrier_name');
      t.enu('lineOfBusiness', ['medicaid', 'medicare', 'commercial'], {
        useNative: true,
        enumName: 'insurance_lob',
      });
      t.text('subLineOfBusiness');
    })
    .then(async () => {
      return knex.schema.table('datasource', (t) => {
        t.unique(['name', 'lineOfBusiness', 'subLineOfBusiness']);
      });
    });
  await Promise.all([memberDatasource, datasource]);
}
