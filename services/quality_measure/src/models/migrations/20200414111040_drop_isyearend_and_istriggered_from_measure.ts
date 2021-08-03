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
  return knex.schema.table('measure', (table) => {
    table.dropColumn('isYearEnd');
    table.dropColumn('isTriggered');
  });
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.table('measure', (table) => {
    table.boolean('isYearEnd');
    table.boolean('isTriggered');
  });

  const updateQueries = measures.map((measure) => {
    return knex('measure')
      .where({ code: measure.code, rateId: 0, deletedAt: null })
      .update({ isYearEnd: true, isTriggered: false });
  });

  await Promise.all(updateQueries);

  return knex.schema.alterTable('measure', (table) => {
    table.boolean('isYearEnd').notNullable().alter();
    table.boolean('isTriggered').notNullable().alter();
  });
}
