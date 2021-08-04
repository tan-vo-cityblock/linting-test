import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('cohort', table => {
    table.renameColumn('assignmentDate', 'goLiveDate');
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('cohort', table => {
    table.renameColumn('goLiveDate', 'assignmentDate');
  });
}
