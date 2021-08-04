import axios from 'axios';
import { transaction } from 'objection';
import { IInsurance } from '../../models/member-insurance';
import { ICreateMemberRequest } from '../../controller/types';
import { setupDb } from '../../lib/test-utils';
import { Member } from '../../models/member';
import {
  createPatientInElation,
  deleteCityblockMemberAttributeInElation,
  deletePatientInElation,
  isElationCallNecessary,
  parseAddresses,
  parseNewInsurances,
  updateCityblockMemberInElation,
} from '../elation';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('elation calls', () => {
  let consolePlaceholder = null as any;
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(Member.knex());
    consolePlaceholder = jest.fn();
    /* tslint:disable no-console */
    console.log = consolePlaceholder;
    /* tslint:enable no-console */
  });

  afterEach(async () => {
    await txn.rollback();
  });

  it('creates a patient', async () => {
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    const req: ICreateMemberRequest = {
      demographics: {
        firstName: 'first',
        lastName: 'last',
        dateOfBirth: '3443',
        isMarkedDeceased: false,
        sex: 'm',
      },
      partner: 'partner',
      insurances: [
        {
          carrier: 'company',
          plans: [
            {
              externalId: '12345',
              details: [
                {
                  lineOfBusiness: 'medicaid',
                },
              ],
            },
          ],
        },
      ],
      primaryPhysician: 2345,
      caregiverPractice: 4543434,
      tags: ['Financial Assistance Program', 'New Patient'],
    };
    const response = await createPatientInElation(req);

    expect(response).not.toBeFalsy();
    expect(axios.post).toBeCalledTimes(2);
    expect(response.id).toBe(1234);
  });

  it('deletes a patient', async () => {
    mockedAxios.delete.mockResolvedValue({});
    const res = await deletePatientInElation('1234');

    expect(res).toBeDefined();
    expect(axios.delete).toBeCalledTimes(1);
  });

  it('updates a patient', async () => {
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    mockedAxios.get.mockResolvedValue({
      data: {
        first_name: 'Sanza',
        last_name: 'Stark',
        sex: 'Female',
        dob: '01/07/1992',
        primary_physician: 9999999,
        caregiver_practice: 9999999,
      },
    });
    mockedAxios.put.mockResolvedValue({ success: true, elationId: '123456789', status: 200 });
    const res = await updateCityblockMemberInElation('a3cef19e-d960-11e9-b350-acde48001122', {
      demographics: {
        firstName: 'Tester',
        isMarkedDeceased: false,
      },
      tags: ['Financial Assistance Program'],
    });
    expect(res).toBeDefined();
    expect(axios.get).toBeCalledTimes(1);
    expect(axios.put).toBeCalledTimes(1);
  });

  it('updates patient tags uniquely', async () => {
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    mockedAxios.get.mockResolvedValue({
      data: {
        first_name: 'Sanza',
        last_name: 'Stark',
        sex: 'Female',
        dob: '01/07/1992',
        primary_physician: 9999999,
        caregiver_practice: 9999999,
        tags: ['Financial Assistance Program', 'New Patient'],
      },
    });
    mockedAxios.put.mockResolvedValue({ success: true, elationId: '123456789', status: 200 });
    const res = await updateCityblockMemberInElation('a3cef19e-d960-11e9-b350-acde48001122', {
      demographics: {
        firstName: 'Tester',
        isMarkedDeceased: false,
      },
      tags: ['Financial Assistance Program', 'Kingdom of the North'],
    });
    expect(res).toBeDefined();
    expect(axios.put).toBeCalledTimes(2);
    expect(axios.get).toBeCalledTimes(2);
  });

  it('does not update a patient', async () => {
    // empty request
    const res = await updateCityblockMemberInElation('a3cef19e-d960-11e9-b350-acde48001122', {});
    expect(res).toBeUndefined();
  });

  it('deletes a patient attribute if it exists', async () => {
    const expectedGetData = {
      first_name: 'Sanza',
      last_name: 'Stark',
      sex: 'Female',
      dob: '01/07/1992',
      primary_physician: 9999999,
      caregiver_practice: 9999999,
      tags: ['Financial Assistance Program', 'New Patient'],
    };
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    mockedAxios.get.mockResolvedValue({ data: expectedGetData });
    mockedAxios.put.mockResolvedValue({ success: true, elationId: '123456789', status: 200 });
    const res = await deleteCityblockMemberAttributeInElation({
      memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
      tags: ['Financial Assistance Program', 'Pay for Service'],
    });
    expect(res).toBeDefined();
    expect(res.statusCode === 200).toBeTruthy();
    expect(axios.get).toBeCalledTimes(3);
    expect(axios.put).toBeCalledTimes(3);
  });

  it('does not fail if patient attribute to be deleted does not exist', async () => {
    const expectedGetData = {
      first_name: 'Sanza',
      last_name: 'Stark',
      sex: 'Female',
      dob: '01/07/1992',
      primary_physician: 9999999,
      caregiver_practice: 9999999,
    };
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    mockedAxios.get.mockResolvedValue({ data: expectedGetData });
    mockedAxios.put.mockResolvedValue({ success: true, elationId: '123456789', status: 200 });
    const res = await deleteCityblockMemberAttributeInElation({
      memberId: 'a3cef19e-d960-11e9-b350-acde48001122',
      tags: ['Financial Assistance Program'],
    });
    expect(res).toBeDefined();
    expect(res.statusCode === 304).toBeTruthy();
    expect(axios.get).toBeCalledTimes(4);
  });

  it('replaces a member`s insurance Id with a new one', async () => {
    const expectedGetData = {
      data: {
        first_name: 'Sanza',
        last_name: 'Stark',
        sex: 'Female',
        dob: '01/07/1992',
        primary_physician: 9999999,
        caregiver_practice: 9999999,
      },
    };
    mockedAxios.post.mockResolvedValue({ data: { id: 1234 } });
    mockedAxios.get.mockResolvedValue(expectedGetData);
    mockedAxios.put.mockResolvedValue({ success: true, elationId: '123456789', status: 200 });
    const res = await updateCityblockMemberInElation('a3cef19e-d960-11e9-b350-acde48001122', {
      insurances: [
        {
          carrier: 'connecticare',
          plans: [
            {
              externalId: 'Kdelta',
              details: [
                {
                  lineOfBusiness: 'Medicare',
                },
              ],
            },
          ],
        },
      ],
    });
    expect(res).toBeDefined();
    expect(axios.get).toBeCalledTimes(5);
    expect(axios.put).toBeCalledTimes(4);
  });
});

describe('elation helpers', () => {
  const requiredFields: ICreateMemberRequest = {
    demographics: {
      firstName: 'first',
      lastName: 'last',
      dateOfBirth: '3443',
      sex: 'm',
      isMarkedDeceased: false,
    },
    partner: 'partner',
    insurances: [
      {
        carrier: 'company',
        plans: [
          {
            externalId: '12345',
            details: [
              {
                lineOfBusiness: 'medicaid',
              },
            ],
          },
        ],
      },
    ],
    primaryPhysician: 2345,
    caregiverPractice: 23312,
  };
  it('parse undefined address', () => {
    expect(parseAddresses(requiredFields.demographics.addresses)).toBeUndefined();
  });
  it('parse semi defined address', () => {
    const cityOnly = {
      demographics: {
        firstName: 'first',
        lastName: 'last',
        dateOfBirth: '3443',
        sex: 'm',
        addresses: [
          {
            city: 'Brooklyn',
          },
        ],
      },
      partner: 'partner',
      insurances: [
        {
          carrier: 'company',
          plans: [
            {
              id: '12345',
              lineOfBusiness: 'medicaid',
            },
          ],
        },
      ],
      primaryPhysician: 2345,
      caregiverPractice: 23312,
    };

    const result = parseAddresses(cityOnly.demographics.addresses);
    expect(result).toBeDefined();
    expect(result.address_line1).toBeUndefined();
    expect(result.address_line2).toBeUndefined();
    expect(result.state).toBeUndefined();
    expect(result.zip).toBeUndefined();
    expect(result.city).toBe('Brooklyn');
  });

  it('parse entire address', () => {
    const fullAddress = {
      demographics: {
        firstName: 'first',
        lastName: 'last',
        dateOfBirth: '3443',
        sex: 'm',
        addresses: [
          {
            street1: '123 Fake St',
            street2: 'Apt 1',
            city: 'Brooklyn',
            state: 'NY',
            zip: '12345',
          },
        ],
      },
      partner: 'partner',
      insurances: [
        {
          carrier: 'company',
          plans: [
            {
              id: '12345',
              lineOfBusiness: 'medicaid',
            },
          ],
        },
      ],
      primaryPhysician: 2345,
      caregiverPractice: 23312,
    };

    const result = parseAddresses(fullAddress.demographics.addresses);
    expect(result).toBeDefined();
    expect(result.address_line1).toBe('123 Fake St');
    expect(result.address_line2).toBe('Apt 1');
    expect(result.state).toBe('NY');
    expect(result.zip).toBe('12345');
    expect(result.city).toBe('Brooklyn');
  });

  it('parses insurance information correctly with two active plans', () => {
    const insurances: IInsurance[] = [
      {
        carrier: 'connecticare',
        plans: [
          {
            externalId: 'newInsurance',
            rank: 'primary',
            current: true,
            details: [
              {
                lineOfBusiness: 'commercial',
                subLineOfBusiness: 'exchange',
                spanDateStart: '2020-01-01',
              },
            ],
          },
          {
            externalId: 'oldInsurance',
            current: true,
            details: [
              {
                lineOfBusiness: 'medicare',
                subLineOfBusiness: 'medicare advantage',
                spanDateStart: '2019-01-01',
                spanDateEnd: '2020-01-01',
              },
            ],
          },
        ],
      },
    ];

    const result = [
      { rank: 'primary', carrier: 'Connecticare, Inc.', member_id: 'newInsurance', plan: null },
      {
        rank: 'primary',
        carrier: 'Connecticare-VIP Medicare Plans',
        member_id: 'oldInsurance',
        plan: null,
      },
    ];

    const parsedInsurance = parseNewInsurances(insurances, null);
    expect(parsedInsurance).toHaveLength(2);
    expect(parsedInsurance).toStrictEqual(result);
  });

  it('parses insurance information correctly with one active and one inactive plan', () => {
    const insurances: IInsurance[] = [
      {
        carrier: 'connecticare',
        plans: [
          {
            externalId: 'newInsurance',
            rank: 'primary',
            current: true,
            details: [
              {
                lineOfBusiness: 'commercial',
                subLineOfBusiness: 'exchange',
                spanDateStart: '2020-01-01',
              },
            ],
          },
          {
            externalId: 'oldInsurance',
            current: false,
            details: [
              {
                lineOfBusiness: 'medicare',
                subLineOfBusiness: 'medicare advantage',
                spanDateStart: '2019-01-01',
                spanDateEnd: '2020-01-01',
              },
            ],
          },
        ],
      },
    ];

    const result = [
      { rank: 'primary', carrier: 'Connecticare, Inc.', member_id: 'newInsurance', plan: null },
    ];

    const parsedInsurance = parseNewInsurances(insurances, null);
    expect(parsedInsurance).toHaveLength(1);
    expect(parsedInsurance).toStrictEqual(result);
  });
});

describe('isElationCallNecessary', () => {
  let oldEnv;

  beforeEach(() => {
    oldEnv = process.env.NODE_ENV;
  });

  afterEach(() => {
    process.env.NODE_ENV = oldEnv;
  });

  it('returns true if the partner is not emblem and its not development', () => {
    process.env.NODE_ENV = 'staging';
    expect(isElationCallNecessary({ partner: 'tufts' } as ICreateMemberRequest)).toEqual(true);
  });

  it('returns false if development', () => {
    process.env.NODE_ENV = 'development';
    expect(isElationCallNecessary({ partner: 'tufts' } as ICreateMemberRequest)).toEqual(false);
  });

  it('returns true if the partner is emblem and request to create in elation is set in the test', () => {
    process.env.NODE_ENV = 'staging';
    expect(
      isElationCallNecessary({
        partner: 'emblem',
        createElationPatient: true,
      } as ICreateMemberRequest),
    ).toEqual(true);
  });

  it('returns false if the partner is emblem and request to create in elation is set in the test', () => {
    process.env.NODE_ENV = 'staging';
    expect(
      isElationCallNecessary({
        partner: 'emblem',
        createElationPatient: false,
      } as ICreateMemberRequest),
    ).toEqual(false);
  });
});
