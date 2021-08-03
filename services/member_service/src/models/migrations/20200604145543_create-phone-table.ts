import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.schema.createTable('phone', (table) => {
    table.uuid('id').unique().defaultTo(knex.raw('uuid_generate_v4()'));
    table
      .uuid('memberId')
      .references('id')
      .inTable('member')
      .onUpdate('CASCADE')
      .onDelete('RESTRICT')
      .notNullable();
    table.string('phone').notNullable();
    table
      .enu('phoneType', ['mobile', 'home', 'main', 'work', 'night', 'fax', 'other'], {
        useNative: true,
        enumName: 'phone_type',
      })
      .defaultTo('main')
      .notNullable();
    table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('deletedAt');
    table.text('deletedReason');
    table.text('updatedBy');
    table.text('deletedBy');
    table.text('clientSource');
  });
}

export async function down(knex: Knex): Promise<any> {
  return Promise.all([knex.schema.dropTableIfExists('phone'), knex.raw('DROP TYPE phone_type')]);
}
