import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('cohort').then(async exists => {
    if (!exists) {
      return knex.schema.createTable('cohort', table => {
        table.increments('id').primary();
        table.string('name').notNullable();
        table.date('assignmentDate');
        table
          .integer('partnerId')
          .references('id')
          .inTable('partner')
          .notNullable();
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

        table.index(['partnerId'], 'index_cohorts_on_partner_id');
      });
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('cohort');
}
