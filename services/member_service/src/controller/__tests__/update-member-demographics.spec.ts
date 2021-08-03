import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { MemberDemographics } from '../../models/member-demographics';
import { updateMemberDemographics } from '../update-member-demographics';
import { updateMemberDemographicsRequest } from './test-data';
import { setupDb } from './test-setup';

describe('Update member demographics route', () => {
  let memberId: string;
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    txn = await transaction.start(Model.knex());

    memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';

    const memberDemographics = {
      firstName: 'Terry',
      middleName: 'T',
      lastName: 'Platypus',
      dateOfBirth: '1980-01-01',
      isMarkedDeceased: false,
      sex: 'male',
      maritalStatus: 'domesticPartner',
      addresses: [],
      phones: [],
      emails: [],
    };
    await MemberDemographics.create(memberId, memberDemographics, txn);
  });

  afterEach(async () => txn.rollback());

  it('should successfully update the member demographics', async () => {
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'PATCH',
      url: `/${API_VERSION}/members/${memberId}/demographics`,
      params: { memberId },
      body: updateMemberDemographicsRequest,
    });

    request.txn = txn;

    await updateMemberDemographics(request, response);

    const _memberId_ = response._getData().memberId;
    const _isMarkedDeceased_ = response._getData().isMarkedDeceased;
    expect(response.statusCode).toBe(200);
    expect(_memberId_).toBe(memberId);
    expect(_isMarkedDeceased_).toBe(true);

    const currentMemberDemographic = await MemberDemographics.getLatestByMemberId(memberId, txn);
    expect(currentMemberDemographic).toMatchObject({
      firstName: 'Jerry',
      lastName: 'Platypus-Platt',
      dateOfBirth: '1975-12-12',
      maritalStatus: 'divorced',
      updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
    });
  });

  it('should save a snapshot of previous demographics info', async () => {
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'POST',
      url: `/${API_VERSION}/members/${memberId}/demographics`,
      params: { memberId },
      body: updateMemberDemographicsRequest,
    });

    request.txn = txn;

    await updateMemberDemographics(request, response);

    const allMemberDemographics = await MemberDemographics.getAllByMemberId(memberId, txn);
    const previousMemberDemographic = allMemberDemographics.filter((md) => !!md.deletedAt)[0];

    expect(previousMemberDemographic.deletedAt).toBeDefined();
    expect(previousMemberDemographic.updatedBy).toBeNull();
    expect(previousMemberDemographic).toMatchObject({
      firstName: 'Terry',
      middleName: 'T',
      lastName: 'Platypus',
      dateOfBirth: '1980-01-01',
      sex: 'male',
      maritalStatus: 'domesticPartner',
    });
  });
});
