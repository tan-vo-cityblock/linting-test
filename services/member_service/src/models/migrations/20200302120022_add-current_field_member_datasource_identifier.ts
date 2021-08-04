import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    // create new column to mark which IDs are active
  await knex.schema.table('member_datasource_identifier', table => {
    table.boolean('current');
  });

  // elation and acpny will always have active IDs if they are not deleted
  await knex('member_datasource_identifier')
    .where({ datasourceId: 2, deletedAt: null })
    .orWhere({ datasourceId: 4, deletedAt: null })
    .update({ current: true });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('member_datasource_identifier', table => {
    table.dropColumn('current');
  });
}
