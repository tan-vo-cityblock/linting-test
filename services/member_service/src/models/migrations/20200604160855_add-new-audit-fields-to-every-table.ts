import * as Knex from 'knex';

// For each of the following tables add 3 new fields necessary for auditing member change history
const tables = [
  'category',
  'cohort',
  'datasource',
  'member',
  'member_datasource_identifier',
  'mrn',
  'partner',
];

export async function up(knex: Knex): Promise<any> {
  // Add the boolean field in the Member Table in order to determine if a member should exist in Commons
  await knex.schema.table('member', (table) => {
    table.boolean('memberInCommons');
  });

  const updatedTables = tables.map(async (t) => {
    return knex.schema.table(t, (table) => {
      table.text('updatedBy');
      table.text('deletedBy');
      table.text('clientSource');
    });
  });

  return Promise.all(updatedTables);
}

export async function down(knex: Knex): Promise<any> {
  // Remove the boolean field in the Member Table (used to determine if a member should exist in Commons)
  await knex.schema.table('member', (table) => {
    table.dropColumn('memberInCommons');
  });

  const originalTables = tables.map(async (t) => {
    return knex.schema.table(t, (table) =>
      table.dropColumns('updatedBy', 'deletedBy', 'clientSource'),
    );
  });

  return Promise.all(originalTables);
}
