import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('market').then(async (exists) => {
    if (!exists) {
      return knex.schema.createTable('market', (table) => {
        table.increments('id');
        table.string('name').unique().notNullable();
        table.string('slug').unique().notNullable();
        table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('market');
}
