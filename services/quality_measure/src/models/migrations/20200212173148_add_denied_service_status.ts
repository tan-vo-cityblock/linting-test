import * as Knex from 'knex';

const denied_service_name = 'deniedService';

export async function up(knex: Knex): Promise<any> {
  return knex('member_measure_status').insert({ name: denied_service_name });
}

export async function down(knex: Knex): Promise<any> {
  return knex('member_measure_status').where('name', denied_service_name).del();
}
