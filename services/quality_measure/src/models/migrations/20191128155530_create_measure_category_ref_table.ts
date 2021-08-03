import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('measure_category').then(async (exists) => {
    if (!exists) {
      return knex.schema.createTable('measure_category', (table) => {
        table.increments('id');
        table.string('name').unique().notNullable();
        table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('measure_category');
}
