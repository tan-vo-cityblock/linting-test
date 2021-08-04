import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('measure').then(async (exists) => {
    if (!exists) {
      await knex.schema.createTable('measure', (table) => {
        table.increments('id');
        table.string('code').notNullable();
        table.integer('rateId').unsigned().defaultTo(0).notNullable();
        table.unique(['code', 'rateId']); // there can be repeated codes each with different rateIds so joining with rateId makes the row unique.
        table.string('name').notNullable().unique();
        table.string('displayName').notNullable();
        table
          .integer('categoryId')
          .unsigned()
          .references('id')
          .inTable('measure_category')
          .onUpdate('CASCADE')
          .onDelete('RESTRICT');
        table.boolean('isYearEnd').notNullable();
        table.boolean('isTriggered').notNullable();
      });
      await knex.raw('ALTER TABLE measure ADD COLUMN "triggerCloseBy" INTERVAL DAY'); // broken up for better visual ordering of columns
      await knex.schema.table('measure', (table) => {
        table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('measure');
}
