import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.createTable('address', (table) => {
    table.uuid('id').unique().defaultTo(knex.raw('uuid_generate_v4()'));
    table
      .uuid('memberId')
      .references('id')
      .inTable('member')
      .onUpdate('CASCADE')
      .onDelete('RESTRICT')
      .notNullable();
    table.string('addressType');
    table.string('street1').notNullable();
    table.string('street2');
    table.string('county');
    table.string('city').notNullable();
    table.string('state').notNullable();
    table.string('zip').notNullable();
    table.timestamp('spanDateStart');
    table.timestamp('spanDateEnd');
    table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('deletedAt');
    table.text('deletedReason');
    table.text('updatedBy');
    table.text('deletedBy');
    table.text('clientSource');
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('address');
}
