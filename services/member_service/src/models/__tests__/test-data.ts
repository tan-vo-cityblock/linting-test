import { MemberInsuranceMapping } from '../member-insurance';

export const queriedMemberFromDatabase = {
  id: 'a3ccab32-d960-11e9-b350-acde48001122',
  phones: [
    ['392fb165-8715-4832-8ade-b1d58d56c6b8', '7185526554', 'main'],
    ['0664695e-55a2-411b-8f81-b94fcf806d08', '5182329951', 'main'],
  ],
  emails: [['cea58957-d96e-4d46-9a2c-a2ffeb96347a', 'bob@live.com']],
  addresses: [
    [
      '9f777b6f-e32b-4a1e-bac7-49d6d7c9dfaf',
      null,
      '25 Rugby Road',
      null,
      null,
      'Brooklyn',
      'NY',
      '11223',
      null,
      null,
    ],
  ],
};

export const expectedAllMapping: MemberInsuranceMapping[] = [
  {
    carrier: 'emblem',
    current: false,
    externalId: 'M1',
    memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M2',
    memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'emblem',
    current: false,
    externalId: 'Z3',
    memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'medicaidNY',
    current: true,
    externalId: 'CIN',
    memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M4',
    memberId: 'a3cdeb0a-d960-11e9-b61b-acde48001122',
  },
  {
    carrier: 'emblem',
    current: false,
    externalId: 'M3',
    memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M8',
    memberId: '6d4c1ad4-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M9',
    memberId: '913cd51e-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M10',
    memberId: '92b58a6c-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M11',
    memberId: '93560f14-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M12',
    memberId: '93c51576-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M13',
    memberId: '942164d4-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M14',
    memberId: '94767a50-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M15',
    memberId: '94e15c62-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M16',
    memberId: '94e15c62-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M17',
    memberId: '95c8a766-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M18',
    memberId: '95c8a766-5cfe-11eb-9dd7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M19',
    memberId: '79c931de-5f5b-11eb-80d7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M20',
    memberId: '79c931de-5f5b-11eb-80d7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M21',
    memberId: '7956188e-5f5b-11eb-80d7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M22',
    memberId: '7890a914-5f5b-11eb-80d7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M23',
    memberId: '7890a914-5f5b-11eb-80d7-acde48001122',
  },
  {
    carrier: 'emblem',
    current: true,
    externalId: 'M24',
    memberId: '54e1d2d0-61b4-11eb-a3a5-3af9d391d31e',
  },
  {
    carrier: 'emblem',
    current: null,
    externalId: 'M5',
    memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
  },
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C1',
    memberId: 'a3ce52a2-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C2',
    memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C3',
    memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'bcbsNC',
    current: null,
    externalId: 'B1',
    memberId: '0d2e4cc6-e153-11e9-988e-acde48001122',
  },
];

export const expectedPartnerMapping: MemberInsuranceMapping[] = [
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C1',
    memberId: 'a3ce52a2-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C2',
    memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'connecticare',
    current: null,
    externalId: 'C3',
    memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
  },
];

export const expectedMemberFilterMapping: MemberInsuranceMapping[] = [
  {
    carrier: 'emblem',
    current: false,
    externalId: 'M3',
    memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
  },
];

export const expectedMrnFilterMapping: MemberInsuranceMapping[] = [
  {
    carrier: 'elation',
    current: true,
    externalId: 'E1',
    memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
  },
  {
    carrier: 'elation',
    current: true,
    externalId: 'E2',
    memberId: 'a3ce52a2-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'elation',
    current: true,
    externalId: 'E4',
    memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
  },
  {
    carrier: 'elation',
    current: true,
    externalId: 'E5',
    memberId: '0d2e4cc6-e153-11e9-988e-acde48001122',
  },
];

test.skip('skip', () => undefined);
