import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('member', table => {
    table
      .uuid('mrnId')
      .references('id')
      .inTable('mrn')
      .onUpdate('CASCADE')
      .onDelete('RESTRICT');
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('member', table => table.dropColumn('mrnId'));
}
