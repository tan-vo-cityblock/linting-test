import * as Knex from 'knex';

interface IMemberMeasureAble {
  id: number;
  sourceId: number;
  setOn: Date;
}

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('member_measure', (table) => {
    table.integer('performanceYear').unsigned();
  });

  const memberMeasuresAble: IMemberMeasureAble[] = await knex
    .select('member_measure.id', 'member_measure.sourceId', 'member_measure.setOn')
    .from('member_measure')
    .innerJoin('member_measure_source', 'member_measure_source.id', 'member_measure.sourceId')
    .whereNull('member_measure.deletedAt')
    .whereNull('member_measure_source.deletedAt')
    .andWhere('member_measure_source.name', 'able');

  const updateQueries = memberMeasuresAble.map((memberMeasure) => {
    return knex('member_measure')
      .where({ id: memberMeasure.id, sourceId: memberMeasure.sourceId, deletedAt: null })
      .update({ performanceYear: memberMeasure.setOn.getUTCFullYear() });
  });

  return Promise.all(updateQueries);
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.table('member_measure', (table) => {
    table.dropColumn('performanceYear');
  });
}
