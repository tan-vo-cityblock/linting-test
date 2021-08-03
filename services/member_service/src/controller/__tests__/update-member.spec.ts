import httpMocks from 'node-mocks-http';
import { find } from 'lodash';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { Address } from '../../models/address';
import { Email } from '../../models/email';
import { Member } from '../../models/member';
import { IInsurance, IInsurancePlan, MemberInsurance } from '../../models/member-insurance';
import { Phone } from '../../models/phone';
import { updateMember } from '../update-member';
import {
  updateMemberContactRequest,
  updateMemberOnlyInsuranceRequest,
  updateMemberZendeskIdRequest
} from './test-data';
import { setupDb } from './test-setup';

describe('Update member route', () => {
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('should update a member\'s zendesk id', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'PATCH',
      url: `/${API_VERSION}/members/${memberId}`,
      params: { memberId },
      body: updateMemberZendeskIdRequest,
    });

    request.txn = txn;

    // tslint:disable-next-line: no-console
    await updateMember(request, response, console.log);

    expect(response.statusCode).toBe(200);

    const updatedMember = await Member.get(memberId, txn);
    expect(updatedMember.zendeskId).toBe('testZendeskId');
  });

  it('should update a member\'s contact info', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'PATCH',
      url: `/${API_VERSION}/members/${memberId}`,
      params: { memberId },
      body: updateMemberContactRequest,
    });

    request.txn = txn;

    // tslint:disable-next-line: no-console
    await updateMember(request, response, console.log);

    expect(response.statusCode).toBe(200);

    const updatedAddresses = await Address.getAllByMemberId(memberId, txn);
    const updatedEmails = await Email.getAllByMemberId(memberId, txn);
    const updatedPhones = await Phone.getAllByMemberId(memberId, txn);
    expect(updatedAddresses).toMatchObject(expect.arrayContaining([
      expect.objectContaining({
        street1: '180 Main St',
        street2: 'Apartment 1',
        city: 'Boston',
        state: 'MA',
        zip: '02105'
      })
    ]));
    expect(updatedEmails).toMatchObject(
      expect.arrayContaining([expect.objectContaining({ email: 'jackiechan@gmail.com' })])
    );
    expect(updatedPhones).toMatchObject(
      expect.arrayContaining([expect.objectContaining({ phone: '3184529078', phoneType: 'mobile' })])
    );
  });

  it('should update a member\'s insurance info', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const response = httpMocks.createResponse();
    const request = httpMocks.createRequest({
      method: 'PATCH',
      url: `/${API_VERSION}/members/${memberId}`,
      params: { memberId },
      body: updateMemberOnlyInsuranceRequest,
    });

    request.txn = txn;

    // tslint:disable-next-line: no-console
    await updateMember(request, response, console.log);

    expect(response.statusCode).toBe(200);

    const updatedInsurances: IInsurance[] = await MemberInsurance.getByMember(memberId, txn);
    const emblemInsurancePlans: IInsurancePlan[] = find(updatedInsurances, { carrier: 'emblem' }).plans;
    expect(emblemInsurancePlans).toMatchObject(
      expect.arrayContaining([
        expect.objectContaining({
          externalId: 'M1',
          rank: 'primary',
          current: true,
          details: expect.arrayContaining([
            expect.objectContaining({
              lineOfBusiness: 'commercial',
              spanDateStart: new Date('2019-01-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-03-01T05:00:00.000Z')
            })
          ])
        }),
        expect.objectContaining({
          externalId: 'MED1',
          rank: 'secondary',
          current: true,
          details: expect.arrayContaining([
            expect.objectContaining({
              lineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-01-01T05:00:00.000Z'),
              spanDateEnd: null
            })
          ])
        })
      ])
    );
  });
});
