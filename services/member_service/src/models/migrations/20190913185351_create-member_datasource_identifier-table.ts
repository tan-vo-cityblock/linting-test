import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('member_datasource_identifier').then(async exists => {
    if (!exists) {
      return knex.schema.createTable('member_datasource_identifier', table => {
        table
          .uuid('id')
          .primary()
          .defaultTo(knex.raw('uuid_generate_v1()'));
        table
          .uuid('memberId')
          .references('id')
          .inTable('member')
          .notNullable();
        table.string('externalId').notNullable();
        table
          .integer('datasourceId')
          .references('id')
          .inTable('datasource')
          .notNullable();
        table
          .timestamp('createdAt')
          .notNullable()
          .defaultTo(knex.fn.now());
        table
          .timestamp('updatedAt')
          .notNullable()
          .defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');

        table.index(['datasourceId'], 'index_member_datasource_identifiers_on_datasource_id');
        table.index(['memberId'], 'index_member_datasource_identifiers_on_member_id');

        table.unique(
          ['datasourceId', 'externalId', 'deletedAt'],
          'index_unique_external_id_per_source',
        );
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('member_datasource_identifier');
}
