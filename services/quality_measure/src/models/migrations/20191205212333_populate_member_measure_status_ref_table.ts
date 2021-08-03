import * as Knex from 'knex';

const measure_statuses = [
  {
    name: 'provOpen',
  },
  {
    name: 'open',
  },
  {
    name: 'provClosed',
  },
  {
    name: 'closed',
  },
  {
    name: 'failed',
  },
  {
    name: 'excluded',
  },
];
export async function up(knex: Knex): Promise<any> {
  return knex('member_measure_status').insert(measure_statuses);
}

export async function down(knex: Knex): Promise<any> {
  const qm_statuses = measure_statuses.map((qm) => qm.name);
  return knex('member_measure_status').whereIn('name', qm_statuses).del();
}
