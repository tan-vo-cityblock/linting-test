import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('datasource', table => {
    table.dropUnique(['name']);
    table.enu('lineOfBusiness', ['medicaid', 'medicare', 'commercial'], {
      useNative: true,
      enumName: 'insurance_lob',
    });
    table.text('subLineOfBusiness');
  });

  return knex.schema.table('datasource', table => {
    table.unique(['name', 'lineOfBusiness', 'subLineOfBusiness']);
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('datasource', table => {
    table.dropUnique(['name', 'lineOfBusiness', 'subLineOfBusiness']);
  });
  await knex.schema.table('datasource', table => {
    table.unique(['name']);
    table.dropColumn('lineOfBusiness');
    table.dropColumn('subLineOfBusiness');
  });
  return knex.raw('DROP TYPE insurance_lob');
}
