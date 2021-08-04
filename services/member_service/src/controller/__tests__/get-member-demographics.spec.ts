import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { getMemberDemographics } from '../../controller/get-member-demographics';
import { emptyMemberDemographicInfo, getMemberDemographicsRequest } from './test-data';
import { setupDb } from './test-setup';

describe('get member-demographics route', () => {
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

  it('should successfully fetch a member demographic by member Id', async () => {
    const memberId = 'a3cdcb8e-d960-11e9-b350-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'GET',
      url: `/${API_VERSION}/members/${memberId}/demographics`,
      params: { memberId },
    });

    request.txn = txn;

    await getMemberDemographics(request, response);

    expect(response.statusCode).toBe(200);
    expect(response._getData()).toMatchObject(getMemberDemographicsRequest);
  });

  it('should successfully return a member demographic with null values instead of a null object', async () => {
    const memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'GET',
      url: `/${API_VERSION}/members/${memberId}/demographics`,
      params: { memberId },
    });

    request.txn = txn;

    await getMemberDemographics(request, response);

    expect(response.statusCode).toBe(200);
    expect(response._getData).toBeDefined();
    expect(response._getData()).toMatchObject(emptyMemberDemographicInfo);
  });
});
