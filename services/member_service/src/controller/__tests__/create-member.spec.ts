import { NextFunction, Request } from 'express';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { API_VERSION } from '../../app';
import { demographicsValidation } from '../../middleware/validate-request';
import { Member } from '../../models/member';
import { createMember } from '../create-member';
import {
  failingCreateRequest,
  failingRequestForElation,
  createMemberRequest,
} from './test-data';
import { setupDb } from './test-setup';

describe('Create member route', () => {
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;
  const clientSource = 'testSource';

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => null);
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('should successfully create a member', async () => {
    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members`,
      body: createMemberRequest,
      headers: { clientSource },
    });

    request.txn = txn;

    await createMember(request, response, next);
    expect(response.statusCode).toBe(200);

    const { patientId, cityblockId } = response._getData();
    expect(response.statusCode).toBe(200);
    expect(validate(patientId)).toBeTruthy();

    const savedMember = await Member.get(patientId, txn);
    expect(cityblockId).toBe(savedMember.cbhId);

    const savedClientSource = await Member.query(txn)
      .select(['clientSource'])
      .where({ id: patientId })
      .first();

    expect(clientSource).toBe(savedClientSource.clientSource);
  });

  it('should fail on validation error', async () => {
    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members`,
      body: failingCreateRequest,
      headers: { clientSource },
    });

    request.txn = txn;

    await createMember(request, response, next);
    expect(response.statusCode).toBe(422);
  });

  it('should fail on elation error if using staging environment and no elation credentials are passed', async () => {
    // First make a deep copy of the env variable in order to revert back after the test
    const oldEnv = Object.assign('', process.env.NODE_ENV);
    process.env.NODE_ENV = 'staging';

    const next = jest.fn() as NextFunction;
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members`,
      body: failingRequestForElation,
      headers: { clientSource },
    });

    request.txn = txn;

    await createMember(request, response, next);
    const errorMsg = `failed to create member in elation [demographics: ${JSON.stringify(failingRequestForElation.demographics)}]`;
    expect(next).toBeCalledWith(new Error(errorMsg));

    // once the test is done, revert the env variable back
    process.env.NODE_ENV = oldEnv;
  });

  it('accepts valid data', async () => {
    const request: Request = {
      body: {
        firstName: 'Dany',
        lastName: 'Targaryen',
        dob: '0297-01-01',
        sex: 'female',
      },
    } as Request;
    const errors = await demographicsValidation(request);
    expect(errors.length).toEqual(0);
  });

  it('allows non-specified sex', async () => {
    const request: Request = {
      body: {
        firstName: 'Drogon',
        lastName: 'Targaryen',
        dob: '0297-01-01',
        sex: null,
      },
    } as Request;
    const errors = await demographicsValidation(request);
    expect(errors.length).toEqual(0);
  });

  it('validates bad birthday', async () => {
    const request: Request = {
      body: {
        firstName: 'Dany',
        lastName: 'Targaryen',
        dob: '297 AC',
        sex: null,
      },
    } as Request;

    const errors = await demographicsValidation(request);

    expect(errors.length).toEqual(1);
    expect(errors).toMatchObject(['DOB must be of the format YYYY-MM-DD']);
  });
});
