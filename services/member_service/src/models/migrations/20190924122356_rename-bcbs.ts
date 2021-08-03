import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex('partner')
    .where({ id: 3 })
    .update({ name: 'bcbsNC' });
}

export async function down(knex: Knex): Promise<any> {
  return knex('partner')
    .where({ id: 3 })
    .update({ name: 'bcbs_nc' });
}
