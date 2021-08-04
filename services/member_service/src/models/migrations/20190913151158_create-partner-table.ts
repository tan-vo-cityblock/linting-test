import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('partner').then(async exists => {
    if (!exists) {
      return knex.schema.createTable('partner', table => {
        table.increments('id').primary();
        table.string('name').unique().notNullable();
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
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('partner');
}
