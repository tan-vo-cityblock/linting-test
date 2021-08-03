import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const stateInsuranceIds = [
    {
      id: 25,
      name: 'medicareNY',
      lineOfBusiness: 'medicare',
    },
    {
      id: 26,
      name: 'medicaidCI',
      lineOfBusiness: 'medicaid',
    },
    {
      id: 27,
      name: 'medicareCI',
      lineOfBusiness: 'medicare',
    },
    {
      id: 28,
      name: 'medicaidMA',
      lineOfBusiness: 'medicaid',
    },
    {
      id: 29,
      name: 'medicareMA',
      lineOfBusiness: 'medicare',
    },
    {
      id: 30,
      name: 'medicaidMD',
      lineOfBusiness: 'medicaid',
    },
    {
      id: 31,
      name: 'medicareMD',
      lineOfBusiness: 'medicare',
    },
  ];

  return Promise.all([
    knex('datasource').where({ id: 8 }).update({ lineOfBusiness: 'medicaid' }),
    knex('datasource').insert(stateInsuranceIds),
  ]);
}

export async function down(knex: Knex): Promise<any> {
  return Promise.all([knex('datasource').whereBetween('id', [25, 31]).del()]);
}
