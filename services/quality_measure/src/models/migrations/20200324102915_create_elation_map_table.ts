import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('elation_map').then(async (exists) => {
    if (!exists) {
      return knex.schema.createTable('elation_map', (table) => {
        table.increments('id');
        table.string('codeType').notNullable();
        table.string('code').notNullable();
        table
          .integer('measureId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('measure')
          .onUpdate('CASCADE')
          .onDelete('RESTRICT');
        table
          .integer('statusId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('member_measure_status')
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
  return knex.schema.dropTableIfExists('elation_map');
}
