import Knex from 'knex';
import { flatMap, isNil, omitBy } from 'lodash';
import {
  BCBSNC_DATASOURCE_ID,
  BCBSNC_PARTNER_ID,
  CCI_COHORT_ID,
  CCI_DATASOURCE_ID,
  CCI_PARTNER_ID,
  EMBLEM_COHORT1_ID,
  EMBLEM_COHORT2_ID,
  EMBLEM_DATASOURCE_ID,
  EMBLEM_PARTNER_ID,
  HIGH_COST_CATEGORY_ID,
  NEW_YORK_MEDICAID_DATASOURCE_ID,
} from '../static-ids';

interface ISeedInsuranceDetails {
  id?: string,
  lineOfBusiness?: string;
  subLineOfBusiness?: string;
  spanDateStart?: string;
  spanDateEnd?: string;
  deletedAt?: string;
}

interface ISeedMemberInsurance {
  id: string;
  externalId: string;
  datasourceId: number;
  rank?: string;
  current?: boolean;
  details: ISeedInsuranceDetails[];
}

interface ISeedMrn {
  id: string;
  mrnName: string;
  mrn: string;
}

interface ISeedMember {
  id: string;
  cbhId: number;
  partnerId: number;
  cohortId: number | null;
  categoryId: number | null;
  insurances: ISeedMemberInsurance[];
  mrn: ISeedMrn;
  zendeskId: string | null;
}

async function createMember(knex: Knex, member: ISeedMember) {
  const { id, cbhId, partnerId, cohortId, categoryId, insurances, mrn, zendeskId } = member;
  let mrnId;

  if (!!mrn) {
    await knex.table('mrn').insert({
      id: mrn.id,
      mrn: mrn.mrn,
      name: mrn.mrnName,
    });

    mrnId = mrn.id;
  }

  await knex.table('member').insert({
    id,
    cbhId,
    partnerId,
    cohortId,
    categoryId,
    mrnId,
    zendeskId,
  });

  await Promise.all(
    insurances.map((insurance) => {
      const memberInsurance = omitBy({ ...insurance, memberId: id, details: undefined }, isNil);
      return knex.table('member_insurance').insert(memberInsurance);
    }),
  );

  await Promise.all(
    flatMap(insurances, (insurance) => {
      return insurance.details.map((detail) => {
        const memberInsuranceDetails = { ...detail, memberDatasourceId: insurance.id };
        return knex.table('member_insurance_details').insert(memberInsuranceDetails);
      });
    }),
  );
}

async function createMembers(knex: Knex) {
  await Promise.all([
    createMember(knex, {
      id: 'a3ccab32-d960-11e9-b350-acde48001122',
      cbhId: 10000004,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'c514831a-940d-45d4-b824-72d46edabb1a',
          externalId: 'M1',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: false,
          details: [
            {
              id: 'd3bce3f6-a938-439a-9139-a36ca4976f2b',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: null,
              spanDateStart: '2019-01-01',
              spanDateEnd: '2020-01-01',
            },
            {
              id: '6e1aca7c-2be7-42ad-a36e-d0d1b8b8e7fe',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: null,
              spanDateStart: '2020-03-01',
              spanDateEnd: '2020-11-01',
            },
            {
              id: '40928c49-04db-449c-bb46-d28e34f7912a',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: null,
              spanDateStart: '2020-12-01',
              spanDateEnd: '2021-02-01',
            },
          ],
        },
        {
          id: '4ab1ebf5-97be-4453-98cf-5611ab0c7795',
          externalId: 'M2',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: null,
              spanDateStart: '2021-01-01',
              spanDateEnd: null,
            },
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: null,
              spanDateStart: '2019-01-01',
              spanDateEnd: null,
              deletedAt: '2020-12-01'
            },
          ],
        },
        {
          id: '778dc534-3b1f-11eb-9022-a683e7329d29',
          externalId: 'Z3',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: false,
          details: [],
        },
        {
          id: 'b5b999fd-80fb-4636-99b8-c9d0a151cacf',
          externalId: 'CIN',
          datasourceId: NEW_YORK_MEDICAID_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: null,
              spanDateStart: null,
              spanDateEnd: null,
            },
          ],
        },
      ],
      mrn: null,
      zendeskId: null,
    }),
    createMember(knex, {
      id: 'a3cdcb8e-d960-11e9-b350-acde48001122',
      cbhId: 10000012,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'ba45df7b-f440-42bd-b95b-6451de4bcb68',
          externalId: 'M3',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: false,
          details: [],
        },
      ],
      mrn: null,
      zendeskId: '419499297375',
    }),
    createMember(knex, {
      id: 'a3cdeb0a-d960-11e9-b61b-acde48001122',
      cbhId: 10000020,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT2_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '4ba8ae96-7c6c-4f2a-ab2b-2b5beef0b49e',
          externalId: 'M4',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [],
        },
      ],
      mrn: null,
      zendeskId: '419499297376',
    }),
    createMember(knex, {
      id: 'a74bcb4a-d964-11e9-8372-acde48001122',
      cbhId: 10000036,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT2_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'ee58aaf4-3b1e-11eb-adc1-0242ac120002',
          externalId: 'M5',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: null,
          details: [],
        },
      ],
      mrn: { id: '4550e604-31c9-426f-acfb-1ae93bb02a54', mrnName: 'elation', mrn: 'E1' },
      zendeskId: 'null',
    }),
    createMember(knex, {
      id: 'a3ce52a2-d960-11e9-b350-acde48001122',
      cbhId: 10000048,
      partnerId: CCI_PARTNER_ID,
      cohortId: CCI_COHORT_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '0238c8d0-0a14-49fe-97a8-e3bd6310b4a7',
          externalId: 'C1',
          datasourceId: CCI_DATASOURCE_ID,
          rank: null,
          current: null,
          details: [],
        },
      ],
      mrn: { id: 'e3a0ec5e-c8fc-4668-9ea9-0bb3b00f0f49', mrnName: 'elation', mrn: 'E2' },
      zendeskId: 'null',
    }),
    createMember(knex, {
      id: 'a3cef19e-d960-11e9-b350-acde48001122',
      cbhId: 10000057,
      partnerId: CCI_PARTNER_ID,
      cohortId: CCI_COHORT_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'dc1f6109-fb79-4259-bb79-40d6edd5803a',
          externalId: 'C2',
          datasourceId: CCI_DATASOURCE_ID,
          rank: null,
          current: null,
          details: [],
        },
        {
          id: 'd59efc5c-1970-4fcb-a0a3-89a450481697',
          externalId: 'C3',
          datasourceId: CCI_DATASOURCE_ID,
          rank: null,
          current: null,
          details: [],
        },
      ],
      mrn: { id: 'c6c589df-b891-4a33-8c4d-14f03c4da155', mrnName: 'elation', mrn: 'E4' },
      zendeskId: null,
    }),
    createMember(knex, {
      id: '0d2e4cc6-e153-11e9-988e-acde48001122',
      cbhId: 10000061,
      partnerId: BCBSNC_PARTNER_ID,
      cohortId: null,
      categoryId: null,
      insurances: [
        {
          id: '25852c00-bf49-45fb-90ac-c65d6e80ce56',
          externalId: 'B1',
          datasourceId: BCBSNC_DATASOURCE_ID,
          rank: null,
          current: null,
          details: [],
        },
      ],
      mrn: { id: 'ab82bb3f-703a-42ec-8de1-2d83641f48e5', mrnName: 'elation', mrn: 'E5' },
      zendeskId: '419499297377',
    }),
    // Members for UpdateInsurance tests
    // with 1 existing details and 1 details in request
    createMember(knex, {
      id: '6d4c1ad4-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125001,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'dc59735b-2ed0-4dbf-ab1b-269dbd3df813',
          externalId: 'M8',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '4d3e60a7-cb2e-4805-a3e6-898bae510344',
            lineOfBusiness: null,
            subLineOfBusiness: null,
            spanDateStart: '2021-01-01',
            spanDateEnd: '2021-02-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 2 existing details and 2 details in request
    createMember(knex, {
      id: '913cd51e-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125017,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'ccdcf38d-36fe-4a50-83d7-978a6f14b053',
          externalId: 'M9',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: '165ab2a7-e20e-4719-b304-f458283be76a',
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: '2020-12-01',
              spanDateEnd: '2021-01-01',
            },
            {
              id: 'ba097c0c-f453-4495-a6df-397ab6b14a70',
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: '2021-02-01',
              spanDateEnd: '2021-03-01',
            },
          ],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 2 existing details and 1 details in request
    createMember(knex, {
      id: '92b58a6c-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125025,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'dee824f2-170d-40cc-ae21-cb5ff0a17a6b',
          externalId: 'M10',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: '2a4b6743-b85b-4117-9d73-f1d1bc11d216',
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: '2020-12-01',
              spanDateEnd: '2021-01-01',
            },
            {
              id: '1bbf06c8-6244-4d46-bc81-0a202d0f77a4',
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: '2021-02-01',
              spanDateEnd: '2021-03-01',
            },
          ],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 1 existing details and 2 details in request 
    createMember(knex, {
      id: '93560f14-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125049,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '793bfc9e-7d24-4a3e-9dc0-0e87565ce099',
          externalId: 'M11',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '02eaac1c-a50a-413d-8654-9ae501f8acbe',
            lineOfBusiness: null,
            subLineOfBusiness: null,
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-02-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 1 existing details and 1 details with new lob/slob in request
    createMember(knex, {
      id: '93c51576-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125058,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'a0e222b4-6e24-4816-a337-bfef1f74a131',
          externalId: 'M12',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '285851f1-bca4-4ab7-9033-d473dec4b5af',
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'exchange',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-02-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 2 existing details and 1 details with updated span date and 1 details with updated lob/slob in request
    createMember(knex, {
      id: '942164d4-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125063,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '21d7fd04-f271-4630-b803-d39590945879',
          externalId: 'M13',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: '1901f275-4cda-4d8e-9ca9-b163701831c7',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: '2020-12-01',
              spanDateEnd: '2021-01-01',
            },
            {
              id: '93f9fab6-385f-4a00-b438-0cb2c34750a1',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: '2021-02-01',
              spanDateEnd: '2021-03-01',
            },
          ],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 1 existing details and 1 identical details in request
    createMember(knex, {
      id: '94767a50-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125074,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '99b3a504-a14e-4d95-bd85-491d0ab03bd1',
          externalId: 'M14',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '07c80708-1229-46c4-9b58-44c99feea183',
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'medicaid',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-04-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // with 1 existing details for each external id 
    createMember(knex, {
      id: '94e15c62-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125082,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: 'c9022418-79c4-49eb-afb4-13fee4b3c806',
          externalId: 'M15',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '5c36581b-99b6-40f8-bc8f-ac8efa202d84',
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'exchange',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-01-01',
          }],
        },
        {
          id: 'e3f62c55-f6c8-42ff-b82f-4be4ffe4bc0b',
          externalId: 'M16',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: 'e0180ded-9408-402d-9a2b-eb23eb023cc0',
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'medicaid',
            spanDateStart: '2021-01-01',
            spanDateEnd: '2021-04-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    createMember(knex, {
      id: '95c8a766-5cfe-11eb-9dd7-acde48001122',
      cbhId: 10125096,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '341e0fde-0875-4e1c-8ffe-910d5a523d90',
          externalId: 'M17',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: 'e1a1708b-013a-42cb-8f9b-27e03268ef5d',
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'exchange',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-01-01',
          }],
        },
        {
          id: 'd75d32ff-4bcf-42ef-b2a1-bb446ac027bd',
          externalId: 'M18',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: '158512f8-4c53-4e56-ba79-5650556eb080',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: '2021-01-01',
              spanDateEnd: '2021-02-01',
            },
            {
              id: 'aa946187-ccc2-4494-9b05-30729e3d235c',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: '2021-03-01',
              spanDateEnd: '2021-05-01',
            },
          ],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    createMember(knex, {
      id: '79c931de-5f5b-11eb-80d7-acde48001122',
      cbhId: 10123409,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '305b8857-bf1e-4058-8ce9-67baa9f025c9',
          externalId: 'M19',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: 'b003591b-5518-490d-8dcc-d213c9863013',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: '2020-11-01',
              spanDateEnd: '2021-01-01',
            },
            {
              id: 'b47cf476-85ab-42fc-bd68-7c6118fd6661',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: '2021-02-01',
              spanDateEnd: '2021-04-01',
            },
          ],
        },
        {
          id: 'f8cc4ff3-fe1f-4765-8697-b2f4969cd453',
          externalId: 'M20',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [
            {
              id: '981dbc6c-727c-4cfa-8ad6-f9ba88df2fb4',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: '2020-08-01',
              spanDateEnd: '2020-11-01',
            },
            {
              id: '9bfbd355-ad99-40ae-afc2-76ba5b2ae4a9',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: '2021-03-01',
              spanDateEnd: '2021-05-01',
            },
          ],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    // reducing request details
    createMember(knex, {
      id: '7956188e-5f5b-11eb-80d7-acde48001122',
      cbhId: 10123394,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '4aab6e52-5eac-4ae6-a284-a0f2b3d05c48',
          externalId: 'M21',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '9b60baa5-0c83-4728-853c-4272c3f90a8e',
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'medicaid',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-01-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    createMember(knex, {
      id: '7890a914-5f5b-11eb-80d7-acde48001122',
      cbhId: 10123383,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '8d6b95c2-0a73-45e9-8724-c2f4a6e98887',
          externalId: 'M22',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: '24bc354c-2ff7-4192-9e5e-66768f70faeb',
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'medicaid',
            spanDateStart: '2020-11-01',
            spanDateEnd: '2021-01-01',
          }],
        },
        {
          id: '8ae00795-63cc-4422-986c-839edb461d7e',
          externalId: 'M23',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: null,
          current: true,
          details: [{
            id: 'ffa7c043-2f0a-4196-81d1-d2729890c547',
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'exchange',
            spanDateStart: '2020-10-01',
            spanDateEnd: '2020-11-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
    createMember(knex, {
      id: '54e1d2d0-61b4-11eb-a3a5-3af9d391d31e',
      cbhId: 10130077,
      partnerId: EMBLEM_PARTNER_ID,
      cohortId: EMBLEM_COHORT1_ID,
      categoryId: HIGH_COST_CATEGORY_ID,
      insurances: [
        {
          id: '022ac593-64c0-4325-9a66-839835f3fef0',
          externalId: 'M24',
          datasourceId: EMBLEM_DATASOURCE_ID,
          rank: 'primary',
          current: true,
          details: [{
            id: '9ee9c679-6368-4dc8-836e-e259873afd84',
            lineOfBusiness: null,
            subLineOfBusiness: null,
            spanDateStart: '2017-06-01',
            spanDateEnd: '2021-01-01',
          }],
        }
      ],
      mrn: null,
      zendeskId: null,
    }),
  ]);
}

async function createDemographic(
  knex: Knex,
  id: string,
  memberId: string,
  firstName: string,
  middleName: string,
  lastName: string,
  dateOfBirth: string,
  dateOfDemise: string,
  sex: string,
  gender: string,
  ethnicity: string,
  maritalStatus: string,
  updatedBy: string,
  ssn: string,
  ssnLastFour: string,
  addresses: any[],
  phones: any[],
  emails: any[],
  race?: string,
  language?: string
) {
  return Promise.all([
    knex.table('member_demographics').insert({
      id,
      memberId,
      firstName,
      middleName,
      lastName,
      dateOfBirth,
      dateOfDemise,
      sex,
      gender,
      ethnicity,
      maritalStatus,
      updatedBy,
      ssn,
      ssnLastFour,
    }),

    knex.table('address').insert(addresses),
    knex.table('phone').insert(phones),
    knex.table('email').insert(emails),
  ]);
}

async function createDemographics(knex: Knex) {
  await Promise.all([
    createDemographic(
      knex,
      '0664695e-55a2-411b-8f81-acde48001122',
      'a3ccab32-d960-11e9-b350-acde48001122',
      'Jackie',
      'Long',
      'Chan',
      '1967-05-07',
      null,
      'male',
      'male',
      null,
      'unknown',
      null,
      null,
      null,
      [
        {
          id: '9f777b6f-e32b-4a1e-bac7-49d6d7c9dfaf',
          memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
          addressType: null,
          street1: '25 Rugby Road',
          street2: null,
          county: null,
          city: 'Brooklyn',
          state: 'NY',
          zip: '11223',
          spanDateStart: null,
          spanDateEnd: null,
        },
      ],
      [
        {
          id: '392fb165-8715-4832-8ade-b1d58d56c6b8',
          memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
          phone: '7185526554',
          phoneType: 'main',
        },
        {
          id: '0664695e-55a2-411b-8f81-b94fcf806d08',
          memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
          phone: '5182329951',
          phoneType: 'main',
        },
      ],
      [
        {
          id: 'cea58957-d96e-4d46-9a2c-a2ffeb96347a',
          memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
          email: 'jackie@chan.com',
        },
      ],
    ),
    createDemographic(
      knex,
      '0664695e-5860-11e9-b350-acde48001122',
      'a3cdcb8e-d960-11e9-b350-acde48001122',
      'Kong-sang',
      null,
      'Chan',
      '1954-04-07',
      null,
      'male',
      'male',
      null,
      'domesticPartner',
      null,
      null,
      null,
      [
        {
          id: '9f777b6f-8715-4832-8ade-49d6d7c9dfaf',
          memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
          addressType: 'residential',
          street1: '15 East Medows',
          street2: 'apt 7b',
          county: null,
          city: 'Flushing',
          state: 'NY',
          zip: '11392',
          spanDateStart: null,
          spanDateEnd: null,
        },
      ],
      [
        {
          id: '392fb165-e32b-4a1e-bac7-b1d58d56c6b8',
          memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
          phone: '7185526554',
          phoneType: 'main',
        },
        {
          id: '0664695e-d96e-4d46-9a2c-b94fcf806d08',
          memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
          phone: '5182329951',
          phoneType: 'main',
        },
      ],
      [
        {
          id: 'cea58957-55a2-411b-8f81-a2ffeb96347a',
          memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
          email: 'bob@live.com',
        },
      ],
      'Asian',
      'English'
    ),
    createDemographic(
      knex,
      '2a531383-de82-4755-8cbc-6a6190d5e075',
      'a3cdeb0a-d960-11e9-b61b-acde48001122',
      'Marvin',
      'the paranoid',
      'Android',
      '1921-10-27',
      null,
      'male',
      'male',
      null,
      'divorced',
      null,
      '090078647',
      '8647',
      [],
      [],
      [],
    ),
  ]);
}

const seed = async (knex: Knex) => {
  await createMembers(knex);
  await createDemographics(knex);
};

exports.seed = async (knex: Knex) => {
  await knex.raw(
    `
      DO
      $func$
      BEGIN
        EXECUTE
        (
          SELECT 'TRUNCATE TABLE ' || string_agg(format('%I.%I', table_schema, table_name), ', ') || ' RESTART IDENTITY CASCADE'
          FROM information_schema.tables
          WHERE table_schema IN ('public')
          AND table_type = 'BASE TABLE'
          AND table_name != 'knex_migrations'
          AND table_name != 'knex_migrations_lock'
          AND table_name != 'damm_matrix'
          AND table_name != 'category'
          AND table_name != 'cohort'
          AND table_name != 'datasource'
          AND table_name != 'partner'
        );
      END
      $func$;
    `,
  );
  return seed(knex);
};
