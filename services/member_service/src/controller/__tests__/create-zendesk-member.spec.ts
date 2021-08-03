import axios from 'axios';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { createZendeskMember } from '../../controller/create-zendesk-member';
import { createZendeskMemberRequest } from './test-data';
import { setupDb } from './test-setup';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('post create-zendesk-member route', () => {
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

  it('should successfully create a member in zendesk', async () => {
    mockedAxios.post.mockResolvedValue({ data: { user: { id: 419691741413 } } });
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members/${memberId}/zendesk`,
      params: { memberId },
      body: createZendeskMemberRequest,
    });

    request.txn = txn;

    await createZendeskMember(request, response);

    expect(response.statusCode).toBe(200);
    expect(response._getData()).toMatchObject({
      id: 'a3ccab32-d960-11e9-b350-acde48001122',
      categoryId: 1,
      cbhId: '10000004',
      cohortId: 1,
      partnerId: 1,
      zendeskId: '419691741413',
    });
  });
});
