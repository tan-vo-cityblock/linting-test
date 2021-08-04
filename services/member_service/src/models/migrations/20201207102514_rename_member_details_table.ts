import * as Knex from "knex";


export async function up(knex: Knex): Promise<any> {
  return knex.schema.renameTable('member_datasource_identifier','member_insurance');
}


export async function down(knex: Knex): Promise<any> {
  return knex.schema.renameTable('member_insurance','member_datasource_identifier');
}

