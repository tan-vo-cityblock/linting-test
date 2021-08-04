import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await Promise.all([
    // Emblem
    knex('member_insurance')
      .whereIn('datasourceId', [10, 11, 12, 13, 14, 15])
      .update({ datasourceId: 1 }),

    // Connecticare
    knex('member_insurance').whereIn('datasourceId', [16, 17, 18, 22]).update({ datasourceId: 3 }),

    // Tufts
    knex('member_insurance').whereIn('datasourceId', [19, 20, 21]).update({ datasourceId: 9 }),

    // Carefirst
    knex('member_insurance').whereIn('datasourceId', [24]).update({ datasourceId: 23 }),
  ]);

  return knex('datasource')
    .whereIn('id', [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24])
    .del();
}

export async function down(knex: Knex): Promise<any> {
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
      subLineOfBusiness: 'fully insured',
    },
  ];

  const connecticareInsurance = [
    {
      id: 16,
      name: 'connecticare',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'fully insured',
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
    {
      id: 22,
      name: 'connecticare',
      lineOfBusiness: 'medicare',
      subLineOfBusiness: 'dual',
    },
  ];

  const tuftsInsurance = [
    {
      id: 19,
      name: 'tufts',
      lineOfBusiness: 'medicare',
      subLineOfBusiness: 'dual',
    },
    {
      id: 20,
      name: 'tufts',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'MCO',
    },
    {
      id: 21,
      name: 'tufts',
      lineOfBusiness: 'commercial',
      subLineOfBusiness: 'direct',
    },
  ];

  const carefirstInsurance = [
    {
      id: 24,
      name: 'carefirst',
      lineOfBusiness: 'medicaid',
      subLineOfBusiness: 'supplemental security income',
    },
  ];

  const partnerDatasources = [emblemInsurance, connecticareInsurance, tuftsInsurance, carefirstInsurance];
  return Promise.all(
    partnerDatasources.map((insurance) => knex.table('datasource').insert(insurance)),
  );
}
