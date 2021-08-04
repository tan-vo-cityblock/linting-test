import * as Knex from 'knex';

const measure_categories = [
  {
    name: 'Wellness',
  },
  {
    name: 'AOD',
  },
  {
    name: 'Well Visit',
  },
  {
    name: 'Cancer Screening',
  },
  {
    name: 'Vaccination',
  },
  {
    name: 'Diabetes',
  },
  {
    name: 'Cardiovascular',
  },
  {
    name: 'Bone Health',
  },
  {
    name: 'Statin',
  },
  {
    name: 'Maternal',
  },
];
export async function up(knex: Knex): Promise<any> {
  return knex('measure_category').insert(measure_categories);
}

export async function down(knex: Knex): Promise<any> {
  const qm_categories = measure_categories.map((qm) => qm.name);
  return knex('measure_category').whereIn('name', qm_categories).del();
}
