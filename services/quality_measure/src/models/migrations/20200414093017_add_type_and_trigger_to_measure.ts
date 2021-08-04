import * as Knex from 'knex';
const measures = [
  {
    code: 'HEDIS ABA',
  },
  {
    code: 'ABLE AWV1',
  },
  {
    code: 'ABLE AWV2',
  },
  {
    code: 'HEDIS FVA',
  },
  {
    code: 'HEDIS FVO',
  },
  {
    code: 'HEDIS BCS',
  },
  {
    code: 'HEDIS CCS',
  },
  {
    code: 'HEDIS COL',
  },
  {
    code: 'HEDIS CDC1',
  },
  {
    code: 'HEDIS CDC5',
  },
  {
    code: 'HEDIS CDC6',
  },
];

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('measure', (table) => {
    table.string('type');
    table.string('trigger');
  });

  // need to use raw to add constraints
  await knex.raw(`
    ALTER TABLE measure 
    ADD CONSTRAINT "measure_type_check" CHECK (type IN ('outcome', 'process')),
    ADD CONSTRAINT "measure_trigger_check" CHECK (trigger IN ('annual', 'event'));
  `);

  const updateQueries = measures.map((measure) => {
    return knex('measure')
      .where({ code: measure.code, rateId: 0, deletedAt: null })
      .update({ type: 'process', trigger: 'annual' });
  });

  await Promise.all(updateQueries);

  // can't set notNullable() until after columns are filled since column is set to null when initially created.
  return knex.schema.alterTable('measure', (table) => {
    table.string('type').notNullable().alter();
    table.string('trigger').notNullable().alter();
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.table('measure', (table) => {
    table.dropColumn('type');
    table.dropColumn('trigger');
  });
}
