import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('member').then(async exists => {
    if (!exists) {
      await knex.schema
        .raw('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
        // Create the sequence used for computing chbId.
        // The start number was chosen by computing the max for the existing data and adding 1.
        .raw('CREATE SEQUENCE cbh_id_sequence START 1012335')
        .createTable('member', table => {
          table
            .uuid('id')
            .primary()
            .defaultTo(knex.raw('uuid_generate_v1()'));
          table
            .bigInteger('cbhId')
            .notNullable()
            .unique()
            .defaultTo(knex.raw(`nextdamm('cbh_id_sequence')`));
          table
            .integer('partnerId')
            .references('id')
            .inTable('partner')
            .notNullable();
          table
            .integer('cohortId')
            .references('id')
            .inTable('cohort')
            .nullable();
          table
            .integer('categoryId')
            .references('id')
            .inTable('category')
            .nullable();
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

          table.index(['partnerId'], 'index_members_on_partner_id');
          table.index(['cohortId'], 'index_members_on_cohort_id');
          table.index(['categoryId'], 'index_members_on_category_id');
        });
      await knex.raw(
        'ALTER TABLE member ADD CONSTRAINT cbh_id_valid_damm CHECK (valid_damm_code("cbhId"))',
      );
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('member').raw('DROP SEQUENCE IF EXISTS cbh_id_sequence');
}
