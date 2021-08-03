import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { createInsurance } from '../create-insurance';
import { createMemberInsuranceRequest } from './test-data';
import { setupDb } from './test-setup';

describe('Create member insurance route', () => {
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('should successfully create a new insurance with details for an existing member', async () => {
    const memberId = 'a3cdcb8e-d960-11e9-b350-acde48001122';
    const next = jest.fn();
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members/${memberId}/insurance`,
      params: { memberId },
      body: createMemberInsuranceRequest,
    });

    request.txn = txn;

    await createInsurance(request, response, next);
    const responseBody = response._getData();

    expect(response.statusCode).toBe(200);
    expect(responseBody).toMatchObject([
      {
        memberId: 'a3cdcb8e-d960-11e9-b350-acde48001122',
        externalId: 'insuranceWithDetails',
        datasourceId: 1,
        current: true,
        rank: 'primary',
        details: expect.arrayContaining([
          expect.objectContaining({
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'fully insured',
            spanDateEnd: new Date('2018-12-31T05:00:00.000Z'),
            spanDateStart: new Date('2018-01-01T05:00:00.000Z'),
          }),
          expect.objectContaining({
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'fully insured',
            spanDateStart: new Date('2019-10-01T04:00:00.000Z'),
          }),
        ]),
      },
    ]);
  });
});
