import { NextFunction } from 'express';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { getMember, getMemberByZendeskId } from '../../controller/get-member';
import { getMembers } from '../../controller/get-members';
import { setupDb } from './test-setup';

describe('get member route', () => {
  let testDb: ReturnType<typeof setupDb>;
  let txn: Transaction;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('should successfully get all members', async () => {
    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'GET',
      url: `/${API_VERSION}/members`,
    });

    request.txn = txn;

    await getMembers(request, response, next);

    const responseData = response._getData();
    expect(response.statusCode).toBe(200);
    expect(responseData.members).toHaveLength(20);
  });

  it('should successfully return the requested member', async () => {
    const memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'GET',
      url: `/${API_VERSION}/members/${memberId}`,
      params: { memberId },
    });

    request.txn = txn;

    await getMember(request, response, next);

    const responseData = response._getData();
    expect(response.statusCode).toBe(200);
    expect(responseData).toMatchObject({
      id: memberId,
      cbhId: '10000036',
      partner: 'emblem',
      cohort: 'Emblem Cohort 2',
    });

  });

  it('should successfully return the requested member through zendesk id', async () => {
    const memberId = 'a3cdeb0a-d960-11e9-b61b-acde48001122';
    const zendeskId = '419499297376';
    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'GET',
      url: `/${API_VERSION}/members/zendesk/${zendeskId}`,
      params: { zendeskId },
    });

    request.txn = txn;

    await getMemberByZendeskId(request, response, next);

    const responseData = response._getData();
    expect(response.statusCode).toBe(200);
    expect(responseData).toMatchObject({
      id: memberId,
      cbhId: '10000020',
      partner: 'emblem',
      cohort: 'Emblem Cohort 2',
    });
  });
});
