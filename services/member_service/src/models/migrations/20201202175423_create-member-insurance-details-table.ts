import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const member_table_exists = await knex.schema.hasTable('member_insurance_details');

  if (!member_table_exists) {
    return knex.schema.createTable('member_insurance_details', (table) => {
      table
        .uuid('id')
        .primary()
        .defaultTo(knex.raw('uuid_generate_v4()'));
      table
        .uuid('memberDatasourceId')
        .references('id')
        .inTable('member_datasource_identifier')
        .notNullable();
      table
        .date('spanDateStart');
      table
        .date('spanDateEnd');
      table.enu('lineOfBusiness', ['medicaid', 'medicare', 'commercial'], {
        useNative: true,
        enumName: 'insurance_line_of_business',
      });
      table.text('subLineOfBusiness');
      table
        .date('createdAt')
        .notNullable()
        .defaultTo(knex.fn.now());
      table
        .timestamp('updatedAt')
        .notNullable()
        .defaultTo(knex.fn.now());
      table.timestamp('deletedAt');
      table.text('deletedReason');
      table.text('updatedBy');
      table.text('deletedBy');
      table.text('clientSource');
      table.index(['memberDatasourceId'], 'index_member_member_insurance_details_on_member_datasource_id');
    });
  }
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.dropTableIfExists('member_insurance_details');
  return knex.raw('DROP TYPE insurance_line_of_business');
}
