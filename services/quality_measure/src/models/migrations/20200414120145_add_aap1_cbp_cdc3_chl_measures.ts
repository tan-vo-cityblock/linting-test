import * as Knex from 'knex';

const measures = [
  {
    code: 'HEDIS AAP1',
    name: 'Adults’ Access to Preventive/Ambulatory Health Services: Medicare and Medicaid',
    displayName: 'Preventive Health Visit',
    categoryName: 'Wellness',
    type: 'process',
    trigger: 'annual',
  },
  {
    code: 'HEDIS CBP',
    name: 'Controlling High Blood Pressure',
    displayName: 'Blood Pressure < 140 / 90',
    categoryName: 'Wellness',
    type: 'outcome',
    trigger: 'annual',
  },
  {
    code: 'HEDIS CDC3',
    name: 'Comprehensive Diabetes Care: HbA1c Control <8.0%',
    displayName: 'Diabetes: A1c < 8.0%',
    categoryName: 'Diabetes',
    type: 'outcome',
    trigger: 'annual',
  },
  {
    code: 'HEDIS CHL',
    name: 'Chlamydia Screening in Women',
    displayName: 'Chlamydia Scr.',
    categoryName: 'Wellness',
    type: 'process',
    trigger: 'annual',
  },
];

export async function up(knex: Knex): Promise<any> {
  const measuresWithCategoryIds = measures.map(async (measure) => {
    const result = await knex
      .select('id')
      .from('measure_category')
      .first()
      .where('name', measure.categoryName);
    delete measure.categoryName;
    return {
      ...measure,
      categoryId: result.id,
    };
  });
  const measuresToInsert = await Promise.all(measuresWithCategoryIds);
  return knex('measure').insert(measuresToInsert);
}

export async function down(knex: Knex): Promise<any> {
  const measureCodeRateIdPairs = measures.map((measure) => [measure.code, 0]);
  return knex('measure').whereIn(['code', 'rateId'], measureCodeRateIdPairs).del();
}
