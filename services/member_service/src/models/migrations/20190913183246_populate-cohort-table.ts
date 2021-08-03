import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const cohorts = [
    {
      id: -4,
      name: 'Emblem Cohort 3b duplicates',
      partnerId: 1,
    },
    {
      id: -2,
      name: 'Emblem Cohort -2',
      partnerId: 1,
    },
    {
      id: 1,
      name: 'Emblem Cohort 1',
      assignmentDate: '2018-07-02',
      partnerId: 1,
    },
    {
      id: 2,
      name: 'Emblem Cohort 2',
      assignmentDate: '2018-09-04',
      partnerId: 1,
    },
    {
      id: 3,
      name: 'Emblem Cohort 3',
      assignmentDate: '2018-12-10',
      partnerId: 1,
    },
    {
      id: 4,
      name: 'Emblem Cohort 3b',
      assignmentDate: '2019-02-04',
      partnerId: 1,
    },
    {
      id: 5,
      name: 'ConnectiCare Cohort 1a',
      assignmentDate: '2019-03-04',
      partnerId: 2,
    },
    {
      id: 6,
      name: 'ConnectiCare Cohort 1b',
      assignmentDate: '2019-05-30',
      partnerId: 2,
    },
    {
      id: 7,
      name: 'Emblem Cohort 3c',
      assignmentDate: '2019-04-23',
      partnerId: 1,
    },
    {
      id: 8,
      name: 'Emblem Cohort 3c',
      assignmentDate: '2019-05-13',
      partnerId: 1,
    },
    {
      id: 9,
      name: 'ConnectiCare Cohort 1c',
      assignmentDate: '2019-07-08',
      partnerId: 2,
    },
    {
      id: 10,
      name: 'ConnectiCare Cohort 1c',
      assignmentDate: '2019-07-08',
      partnerId: 2,
    },
    {
      id: 11,
      name: 'Emblem Cohort 4a',
      assignmentDate: '2019-07-22',
      partnerId: 1,
    },
  ];

  return knex('cohort').insert(cohorts);
}

export async function down(knex: Knex): Promise<any> {
  return knex('cohort').del();
}
