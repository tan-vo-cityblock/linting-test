import * as Knex from 'knex';
import { flatten } from 'lodash';

interface IElationTagsMap {
  measureCode: string;
  statusName: string;
  elationTags: Array<{ codeType: string; code: string }>;
}

interface IFormattedElationTagsMap {
  codeType: string;
  code: string;
  measureId: number;
  statusId: number;
}

const cdc5 = 'HEDIS CDC5';
const cdc1 = 'HEDIS CDC1';
const fva = 'HEDIS FVA';
const fvo = 'HEDIS FVO';
const aba = 'HEDIS ABA';
const cdc6 = 'HEDIS CDC6';
const awv1 = 'ABLE AWV1';
const awv2 = 'ABLE AWV2';
const ccs = 'HEDIS CCS';
const bcs = 'HEDIS BCS';

const excluded = 'excluded';
const closed = 'closed';

const cptii = 'cptii';
const cpt = 'cpt';
const icd10 = 'icd10';
const hcpcs = 'hcpcs';
const loinc = 'loinc';
const snomed = 'snomed';

const elationTagsMap: IElationTagsMap[] = [
  {
    measureCode: cdc5,
    statusName: closed,
    elationTags: [
      {
        codeType: cptii,
        code: '2022F',
      },
    ],
  },
  {
    measureCode: cdc5,
    statusName: excluded,
    elationTags: [
      {
        codeType: icd10,
        code: 'O24.419',
      },
      {
        codeType: icd10,
        code: 'E09.9',
      },
    ],
  },
  {
    measureCode: cdc1,
    statusName: closed,
    elationTags: [
      {
        codeType: cpt,
        code: '3044F',
      },
      {
        codeType: cpt,
        code: '3045F',
      },
      {
        codeType: cpt,
        code: '3046F',
      },
    ],
  },
  {
    measureCode: cdc1,
    statusName: excluded,
    elationTags: [
      {
        codeType: icd10,
        code: 'O24.419',
      },
      {
        codeType: icd10,
        code: 'E09.9',
      },
    ],
  },
  {
    measureCode: fva,
    statusName: closed,
    elationTags: [
      {
        codeType: cpt,
        code: '90471',
      },
      {
        codeType: cpt,
        code: 'G8482',
      },
      {
        codeType: hcpcs,
        code: 'G8482',
      },
    ],
  },
  {
    measureCode: fva,
    statusName: excluded,
    elationTags: [
      {
        codeType: cpt,
        code: 'G8483',
      },
      {
        codeType: hcpcs,
        code: 'G8483',
      },
    ],
  },
  {
    measureCode: fvo,
    statusName: closed,
    elationTags: [
      {
        codeType: cpt,
        code: 'G0008',
      },
      {
        codeType: cpt,
        code: 'G8482',
      },
      {
        codeType: hcpcs,
        code: 'G8482',
      },
    ],
  },
  {
    measureCode: fvo,
    statusName: excluded,
    elationTags: [
      {
        codeType: cpt,
        code: 'G8483',
      },
      {
        codeType: hcpcs,
        code: 'G8483',
      },
    ],
  },
  {
    measureCode: aba,
    statusName: closed,
    elationTags: [
      {
        codeType: cptii,
        code: '3008F',
      },
      {
        codeType: icd10,
        code: 'Z68.51',
      },
      {
        codeType: icd10,
        code: 'Z68.52',
      },
      {
        codeType: icd10,
        code: 'Z68.53',
      },
      {
        codeType: icd10,
        code: 'Z68.54',
      },
    ],
  },
  {
    measureCode: aba,
    statusName: excluded,
    elationTags: [
      {
        codeType: icd10,
        code: 'O00.00',
      },
    ],
  },
  {
    measureCode: cdc6,
    statusName: closed,
    elationTags: [
      {
        codeType: loinc,
        code: '5804-0',
      },
      {
        codeType: loinc,
        code: '14959-1',
      },
      {
        codeType: icd10,
        code: 'N18.6',
      },
      {
        codeType: icd10,
        code: 'N18.4',
      },
      {
        codeType: cpt,
        code: '4010F',
      },
      {
        codeType: icd10,
        code: 'E08.21',
      },
    ],
  },
  {
    measureCode: awv1,
    statusName: closed,
    elationTags: [
      {
        codeType: cpt,
        code: 'G0438',
      },
      {
        codeType: cpt,
        code: 'G0439',
      },
    ],
  },
  {
    measureCode: ccs,
    statusName: closed,
    elationTags: [
      {
        codeType: loinc,
        code: '18500-9',
      },
    ],
  },
  {
    measureCode: ccs,
    statusName: excluded,
    elationTags: [
      {
        codeType: icd10,
        code: 'Z90.710',
      },
      {
        codeType: icd10,
        code: 'Z90.712',
      },
      {
        codeType: snomed,
        code: '385763009',
      },
    ],
  },
  {
    measureCode: bcs,
    statusName: closed,
    elationTags: [
      {
        codeType: hcpcs,
        code: 'G9899',
      },
    ],
  },
  {
    measureCode: bcs,
    statusName: excluded,
    elationTags: [
      {
        codeType: icd10,
        code: 'Z90.13',
      },
      {
        codeType: icd10,
        code: 'Z90.12',
      },
      {
        codeType: snomed,
        code: '385763009',
      },
    ],
  },
  {
    measureCode: awv2,
    statusName: closed,
    elationTags: [
      {
        codeType: cpt,
        code: '99385',
      },
      {
        codeType: cpt,
        code: '99386',
      },
      {
        codeType: cpt,
        code: '99387',
      },
      {
        codeType: cpt,
        code: '99395',
      },
      {
        codeType: cpt,
        code: '99396',
      },
      {
        codeType: cpt,
        code: '99397',
      },
    ],
  },
];

async function formatElationTagsMap(
  knex: Knex,
  tagsMap: IElationTagsMap[],
): Promise<IFormattedElationTagsMap[][]> {
  const formattedElationTagsMapPromises = tagsMap.map(async (item) => {
    const measure = await knex
      .select('id')
      .from('measure')
      .whereNull('deletedAt')
      .andWhere('code', item.measureCode)
      .andWhere('rateId', 0)
      .first();

    const status = await knex
      .select('id')
      .from('member_measure_status')
      .whereNull('deletedAt')
      .andWhere('name', item.statusName)
      .first();

    return item.elationTags.map((tag) => ({
      ...tag,
      measureId: measure.id,
      statusId: status.id,
    }));
  });

  return Promise.all(formattedElationTagsMapPromises);
}
export async function up(knex: Knex): Promise<any> {
  const formattedElationTagMapNested = await formatElationTagsMap(knex, elationTagsMap);
  const formattedElationTagMap = flatten(formattedElationTagMapNested);
  return knex('elation_map').insert(formattedElationTagMap);
}

export async function down(knex: Knex): Promise<any> {
  const formattedElationTagMapNested = await formatElationTagsMap(knex, elationTagsMap);
  const formattedElationTagMap = flatten(formattedElationTagMapNested);

  const elationCodeTypeAndCodes = formattedElationTagMap.map((tag) => [tag.codeType, tag.code]);
  return knex('elation_map').whereIn(['codeType', 'code'], elationCodeTypeAndCodes).del();
}
