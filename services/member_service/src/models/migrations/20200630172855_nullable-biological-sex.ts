import * as Knex from "knex";


export async function up(knex: Knex): Promise<any> {
  return knex.raw('ALTER TABLE member_demographics ALTER COLUMN sex DROP NOT NULL;')
}


export async function down(knex: Knex): Promise<any> {
  return knex.raw('ALTER TABLE member_demographics ALTER COLUMN sex SET NOT NULL;')
}

