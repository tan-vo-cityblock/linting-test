import * as Knex from 'knex';

const hedisFluMeasures = [
  {
    code: 'HEDIS FVA',
  },
  {
    code: 'HEDIS FVO',
  },
];

export async function up(knex: Knex): Promise<any> {
  const softDeleteHedisFlu = hedisFluMeasures.map((measure) => {
    return knex('measure')
      .where({ code: measure.code, rateId: 0, deletedAt: null })
      .update({
        deletedAt: new Date(),
        deletedReason: 'Flu codes/measureIds changed in Able results',
      });
  });

  return Promise.all(softDeleteHedisFlu);
}

export async function down(knex: Knex): Promise<any> {
  const undoSoftDeleteHedisFlu = hedisFluMeasures.map((measure) => {
    return knex('measure')
      .where({ code: measure.code, rateId: 0 })
      .whereNotNull('deletedAt')
      .update({ deletedAt: null, deletedReason: null });
  });

  return Promise.all(undoSoftDeleteHedisFlu);
}
