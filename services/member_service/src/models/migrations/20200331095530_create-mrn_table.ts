import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.createTable('mrn', table => {
    table
      .uuid('id')
      .unique()
      .defaultTo(knex.raw('uuid_generate_v4()'));
    table.string('mrn').notNullable();
    table
      .enu('name', ['elation', 'acpny'], { useNative: true, enumName: 'mrn_name' })
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
  });

  await knex.raw(
    'create UNIQUE INDEX index_unique_mrn_id_per_source on mrn ("name", "mrn")  WHERE "deletedAt" IS NULL',
  );
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.dropTableIfExists('mrn');
  await knex.raw('DROP TYPE mrn_name');
}