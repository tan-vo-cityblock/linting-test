import * as Knex from 'knex';

const table = 'member_datasource_identifier';
const startDate = '"spanDateStart"';
const endDate = '"spanDateEnd"';

export async function up(knex: Knex): Promise<any> {
  return Promise.all([
    knex.raw(
      `ALTER TABLE ${table} ALTER COLUMN ${startDate} TYPE TIMESTAMP with time zone USING ${startDate}::TIMESTAMP;`,
    ),
    knex.raw(
      `ALTER TABLE ${table} ALTER COLUMN ${endDate} TYPE TIMESTAMP with time zone USING ${endDate}::TIMESTAMP;`,
    ),
  ]);
}

export async function down(knex: Knex): Promise<any> {
  return Promise.all([
    knex.raw(`ALTER TABLE ${table} ALTER COLUMN ${startDate} TYPE DATE USING ${startDate}::DATE;`),
    knex.raw(`ALTER TABLE ${table} ALTER COLUMN ${endDate} TYPE DATE USING ${endDate}::DATE;`),
  ]);
}
