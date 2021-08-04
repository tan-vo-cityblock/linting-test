import * as Knex from 'knex';

const measures = [
  {
    code: 'HEDIS ABA',
    name: 'Adult BMI Assessment',
    displayName: 'BMI',
    categoryName: 'Wellness',
  },
  {
    code: 'ABLE AWV1',
    name: 'Annual Wellness Visit: Medicare',
    displayName: 'Well Visit',
    categoryName: 'Well Visit',
  },
  {
    code: 'ABLE AWV2',
    name: 'Annual Wellness Visit: Commercial & Medicaid',
    displayName: 'Well Visit',
    categoryName: 'Well Visit',
  },
  {
    code: 'HEDIS FVA',
    name: 'Flu Vaccinations for Adults Ages 18-64',
    displayName: 'Flu Vaccine',
    categoryName: 'Vaccination',
  },
  {
    code: 'HEDIS FVO',
    name: 'Flu Vaccinations for Adults Ages 65 and Older',
    displayName: 'Flu Vaccine',
    categoryName: 'Vaccination',
  },
  {
    code: 'HEDIS BCS',
    name: 'Breast Cancer Screening',
    displayName: 'Breast Cancer Scr.',
    categoryName: 'Cancer Screening',
  },
  {
    code: 'HEDIS CCS',
    name: 'Cervical Cancer Screening',
    displayName: 'Cervical Cancer Scr.',
    categoryName: 'Cancer Screening',
  },
  {
    code: 'HEDIS COL',
    name: 'Colorectal Cancer Screening',
    displayName: 'Colorectal Cancer Scr.',
    categoryName: 'Cancer Screening',
  },
  {
    code: 'HEDIS CDC1',
    name: 'Comprehensive Diabetes Care: HbA1c Testing',
    displayName: 'Diabetes: HbA1c Testing',
    categoryName: 'Diabetes',
  },
  {
    code: 'HEDIS CDC5',
    name: 'Comprehensive Diabetes Care: Eye Exam',
    displayName: 'Diabetes: Eye Exam',
    categoryName: 'Diabetes',
  },
  {
    code: 'HEDIS CDC6',
    name: 'Comprehensive Diabetes Care: Medical Attention for Nephropathy',
    displayName: 'Diabetes: Nephropathy',
    categoryName: 'Diabetes',
  },
];

export async function up(knex: Knex): Promise<any> {
  const measuresWithCategoryIds = measures.map(async (qm) => {
    const result = await knex
      .select('id')
      .from('measure_category')
      .first()
      .where('name', qm.categoryName);
    return {
      categoryId: result.id,
      code: qm.code,
      name: qm.name,
      displayName: qm.displayName,
      isYearEnd: true,
      isTriggered: false,
    };
  });
  const measuresToInsert = await Promise.all(measuresWithCategoryIds);
  return knex('measure').insert(measuresToInsert);
}

export async function down(knex: Knex): Promise<any> {
  const measureCodeRateIdPairs = measures.map((measure) => [measure.code, 0]);
  return knex('measure').whereIn(['code', 'rateId'], measureCodeRateIdPairs).del();
}
