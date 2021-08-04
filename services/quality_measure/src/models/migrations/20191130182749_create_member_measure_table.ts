import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('member_measure').then(async (exists) => {
    if (!exists) {
      return knex.schema.createTable('member_measure', (table) => {
        table.bigIncrements('id');
        table.uuid('memberId').notNullable();
        table
          .integer('measureId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('measure')
          .onUpdate('CASCADE')
          .onDelete('RESTRICT');
        table.index(['memberId', 'measureId', 'deletedAt']);
        table
          .integer('sourceId')
          .unsigned()
          .notNullable()
          .references('id')
          .inTable('member_measure_source')
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
        table.text('reason');
        table.timestamp('setOn').notNullable();
        table.timestamp('lastConfirmed').notNullable().defaultTo(knex.fn.now());
        table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
        table.timestamp('deletedAt');
        table.text('deletedReason');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('member_measure');
}
