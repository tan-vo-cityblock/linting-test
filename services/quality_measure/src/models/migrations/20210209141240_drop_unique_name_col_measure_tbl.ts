import * as Knex from 'knex';

const newIndexName = 'measure_name_unique_deleted_at_null';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.alterTable('measure', (table) => {
    table.dropUnique(['name']);
  });
  return knex.raw(
    `CREATE UNIQUE INDEX ${newIndexName} ON measure (name) WHERE "deletedAt" IS NULL;`,
  );
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.alterTable('measure', (table) => {
    table.dropIndex([], newIndexName);
    table.unique(['name']);
  });
}
