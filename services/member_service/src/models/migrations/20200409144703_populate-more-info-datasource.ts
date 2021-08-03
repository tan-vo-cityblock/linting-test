import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const emblemInsurance = [
    {
      id: 10,
      name: 'emblem',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'HARP',
    },
    {
      id: 11,
      name: 'emblem',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'medicaid',
    },
    {
      id: 12,
      name: 'emblem',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'DSNP',
    },
    {
      id: 13,
      name: 'emblem',
      lineOfBusiness: 'medicare',
      subLineOfBusiness: 'medicare advantage',
    },
    {
      id: 14,
      name: 'emblem',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'exchange',
    },
    {
      id: 15,
      name: 'emblem',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'fully exchanged',
    },
  ];

  const connecticareInsurance = [
    {
      id: 16,
      name: 'connecticare',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'fully insured off exchange',
    },
    {
      id: 17,
      name: 'connecticare',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'exchange',
    },
    {
      id: 18,
      name: 'connecticare',
      lineOfBusiness: 'medicare',
      subLineOfBusiness: 'medicare advantage',
    },
  ];

  const tuftsInsurance = [
    {
      id: 19,
      name: 'tufts',
      lineOfBusiness: 'medicare',
      subLineOfBusiness: 'dual',
    },
  ];

  const partnerDatasources = [emblemInsurance, connecticareInsurance, tuftsInsurance];
  return Promise.all(
    partnerDatasources.map(insurance => knex.table('datasource').insert(insurance)),
  );
}

export async function down(knex: Knex): Promise<any> {
  await knex
    .table('datasource')
    .select()
    .whereBetween('id', [10, 19])
    .del();
}
