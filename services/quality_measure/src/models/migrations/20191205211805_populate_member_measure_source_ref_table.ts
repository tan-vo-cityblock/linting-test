import * as Knex from 'knex';

const measure_sources = [
  {
    name: 'able',
  },
  {
    name: 'commons',
  },
  {
    name: 'elation',
  },
  {
    name: 'qm_service',
  },
];
export async function up(knex: Knex): Promise<any> {
  return knex('member_measure_source').insert(measure_sources);
}

export async function down(knex: Knex): Promise<any> {
  const qm_sources = measure_sources.map((qm) => qm.name);
  return knex('member_measure_source').whereIn('name', qm_sources).del();
}
