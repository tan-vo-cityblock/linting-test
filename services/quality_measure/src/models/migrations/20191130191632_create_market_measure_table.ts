import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('market_measure').then(async (exists) => {
    if (!exists) {
      return knex.schema.createTable('market_measure', (table) => {
        table.increments('id');
        table
          .integer('marketId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('market')
          .onUpdate('CASCADE')
          .onDelete('RESTRICT');
        table
          .integer('measureId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('measure')
          .onUpdate('CASCADE')
          .onDelete('RESTRICT');
        table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('market_measure');
}
