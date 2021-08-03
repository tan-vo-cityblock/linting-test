import * as Knex from 'knex';

const measures = [
  {
    code: 'ABLE FVA',
    name: 'Flu Vaccinations for Adults Ages 18-64',
    displayName: 'Flu Vaccine',
    categoryName: 'Vaccination',
    type: 'process',
    trigger: 'annual',
  },
  {
    code: 'ABLE FVO',
    name: 'Flu Vaccinations for Adults Ages 65 and Older',
    displayName: 'Flu Vaccine',
    categoryName: 'Vaccination',
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
