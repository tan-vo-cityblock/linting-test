import axios from 'axios';
import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import {
  constructZenDeskUser,
  createZenDeskUser,
  deleteZendeskUser,
  formatEmail,
  formatPhone,
  IZendeskRequest,
  IZendeskUserCreateRequest,
} from '../zendesk';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('zendesk utils', () => {
  let consolePlaceholder = jest.fn();
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => (testDb = setupDb()));

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
    consolePlaceholder = jest.fn();
    /* tslint:disable no-console */
    console.log = consolePlaceholder;
    /* tslint:enable no-console */
  });

  afterEach(async () => txn.rollback());

  const member1 = 'a3cdeb0a-d960-11e9-b61b-acde48001122';
  const member2 = 'a3ccab32-d960-11e9-b350-acde48001122';
  const member3 = 'a3cef19e-d960-11e9-b350-acde48001122';
  const req: IZendeskRequest = {
    organizationName: 'Emblem Medicaid Virtual',
    primary_hub: 'emblem_medicaid_virtual',
    care_model: 'community_integrated_care',
    primary_chp: 'Murli Prasad Sharma',
  };

  it('constructZenDeskUser parses existing user info', async () => {
    const response: IZendeskUserCreateRequest = await constructZenDeskUser(req, member1, txn);

    expect(response).toMatchObject({
      active: true,
      external_id: 'a3cdeb0a-d960-11e9-b61b-acde48001122',
      locale: 'en-US',
      name: 'Marvin t. Android',
      organization_id: 370453498174,
      role: 'end-user',
      tags: ['community_integrated_care', 'emblem_medicaid_virtual'],
      ticket_restriction: 'requested',
      user_fields: {
        care_model: 'community_integrated_care',
        commons_link:
          'https://commons.cityblock.com/members/a3cdeb0a-d960-11e9-b61b-acde48001122/360',
        date_of_birth: '1921-10-27',
        insurance_carrier: 'emblem',
        partner_id: 'M4',
        primary_chp: 'Murli Prasad Sharma',
        primary_hub: 'emblem_medicaid_virtual',
      },
      verified: true,
    });
  });

  it('constructZenDeskUser selects the latest for a member with multiple insurances', async () => {
    const response: IZendeskUserCreateRequest = await constructZenDeskUser(req, member2, txn);

    expect(response).toMatchObject({
      active: true,
      external_id: 'a3ccab32-d960-11e9-b350-acde48001122',
      locale: 'en-US',
      name: 'Jackie L. Chan',
      organization_id: 370453498174,
      role: 'end-user',
      tags: ['community_integrated_care', 'emblem_medicaid_virtual'],
      ticket_restriction: 'requested',
      user_fields: {
        care_model: 'community_integrated_care',
        commons_link:
          'https://commons.cityblock.com/members/a3ccab32-d960-11e9-b350-acde48001122/360',
        date_of_birth: '1967-05-07',
        insurance_carrier: 'emblem',
        partner_id: 'M2',
        primary_chp: 'Murli Prasad Sharma',
        primary_hub: 'emblem_medicaid_virtual',
      },
      verified: true,
    });
  });

  it('constructZenDeskUser throws an exception if user does not have a "current" insurance id', async () => {
    await expect(constructZenDeskUser(req, member3, txn)).rejects.toThrowError(
      TypeError("Cannot destructure property `carrier` of 'undefined' or 'null'."),
    );
  });

  it('createZenDeskUser creates an end-user', async () => {
    mockedAxios.post.mockResolvedValue({ data: { user: { id: '419691741413', active: true } } });
    const response = await createZenDeskUser(req, member1, txn);
    expect(response).not.toBeFalsy();
    expect(axios.post).toBeCalledTimes(1);
    expect(response.user.id).toBe('419691741413');
    expect(response.user.active).toBeTruthy();
  });

  it('deleteZendeskUser deletes an end-user', async () => {
    mockedAxios.delete.mockResolvedValue({ data: { user: { id: '419691741413', active: false } } });
    const response = await deleteZendeskUser('419691741413');
    expect(response).not.toBeFalsy();
    expect(axios.delete).toBeCalledTimes(1);
    expect(response.user.id).toBe('419691741413');
    expect(response.user.active).toBeFalsy();
  });

  describe('formatEmail', () => {
    it('should return null for if an email is already associated with a zendesk user', async () => {
      const memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
      const email = 'bob@live.com';
      const formatted = await formatEmail(email, memberId, txn);
      expect(formatted).toBeNull();
    });

    it('should return the email if no zendesk user is associated with it', async () => {
      const memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
      const email = 'bobnew@live.com';
      const formatted = await formatEmail(email, memberId, txn);
      expect(formatted).toBe('bobnew@live.com');
    });
  });

  describe('formatPhone', () => {
    it('appends +1 to number if not there', async () => {
      const phone = '9179915587';
      const formatted = formatPhone(phone);
      expect(formatted).toBe('+19179915587');
    });

    it('does not appends +1 to number if there', async () => {
      const phone = '+19179915587';
      const formatted = formatPhone(phone);
      expect(formatted).toBe('+19179915587');
    });
  });
});
